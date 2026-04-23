# Trabajo Práctico Coordinación - Sistemas Distribuidos I (TA050)

### Alumno: Thiago Fernando Baez - Padrón: 110703
 
## Informe: Coordinación y Escalabilidad del Sistema


### Ejecución

`make up` : Inicia los contenedores del sistema y comienza a seguir los logs de todos ellos en un solo flujo de salida.

`make down`:   Detiene los contenedores y libera los recursos asociados.

`make logs`: Sigue los logs de todos los contenedores en un solo flujo de salida.

`make test`: Inicia los contenedores del sistema, espera a que los clientes finalicen, compara los resultados con una ejecución serial y detiene los contenederes.

`make switch`: Permite alternar rápidamente entre los archivos de docker compose de los distintos escenarios provistos.


## 1. Coordinación entre Sum y Aggregation

### Distribución de Datos

La coordinación entre instancias de **Sum** y **Aggregation** utiliza un esquema de **routing determinístico basado en hash**:

```python
    client_hash = sum((i + 1) * ord(c) for i, c in enumerate(str(client_id)))
    fruit_hash = sum(ord(c) for c in fruit)
    return (client_hash + fruit_hash) % AGGREGATION_AMOUNT
```

La idea es transformar la tupla `(client_id, fruit)` en un número entero estable para decidir a qué instancia de Aggregation se envía el dato.

1. `str(client_id)` normaliza el identificador para soportar tanto IDs numéricos como UUIDs.
2. `client_hash` suma el código ASCII de cada carácter del `client_id`, ponderado por su posición (`i + 1`).
3. `fruit_hash` suma los códigos ASCII del nombre de la fruta.
4. Se combinan ambos hashes y se aplica módulo con `AGGREGATION_AMOUNT`.

El índice final es:

```python
index = (hash(client_id) + hash(fruit)) % AGGREGATION_AMOUNT
```

Esto aporta tres propiedades importantes:

- Determinismo: la misma pareja `(cliente, fruta)` siempre cae en el mismo Aggregator.
- Particionado por clave compuesta: dos clientes con la misma fruta no necesariamente caen en el mismo Aggregator.
- Balance práctico: para datos variados, la distribución entre Aggregators tiende a ser razonablemente uniforme.

Ejemplo rápido: si `AGGREGATION_AMOUNT = 4` y el cálculo da `index = 2`, entonces el mensaje se enruta a `AGGREGATION_PREFIX_2`.

Esto garantiza que:
- Una misma fruta de un cliente siempre se envía al mismo Aggregator
- Los datos se distribuyen uniformemente entre Aggregators
- No hay procesamiento redundante en Aggregation (cada fruta de cada cliente va a exactamente un Aggregator)

### Sincronización de EOF

**Múltiples instancias de Sum** requieren coordinación para garantizar que todos los Sums procesen el EOF de cada cliente y envíen sus datos acumulados a los Aggregators.

#### Flujo de transmisión del EOF

1. **Un Sum recibe EOF del cliente** (por `INPUT_QUEUE`):
   - Acumula todos sus datos para ese cliente en `amount_by_client`
   - Marca el cliente como procesado: `processed_clients.add(client_id)`
   - **Envía sus datos acumulados** a todos los Aggregators (routing por hash)
   - **Envía mensajes de EOF** a todos los Aggregators (un EOF por cada instancia de Aggregation)
   - Si hay múltiples Sums (`SUM_AMOUNT > 1`), **realiza un broadcast** enviando el `client_id` por el `SUM_CONTROL_EXCHANGE`

2. **Otros Sums reciben el broadcast de EOF** (por `SUM_CONTROL_EXCHANGE`):
   - Verifican si ya procesaron ese cliente (consultando `processed_clients`)
   - Si no lo procesaron:
     - Marcan el cliente como procesado
     - Envían sus datos acumulados a todos los Aggregators
     - Envían sus mensajes de EOF a todos los Aggregators
   - **No re-envían el broadcast** (evita ciclos infinitos)
   - Si ya fue procesado, simplemente descartan el mensaje

#### Por qué funciona

- Cada Aggregator recibe **exactamente `SUM_AMOUNT` mensajes de EOF por cliente** (uno de cada Sum)
- Esto garantiza que todos los Sums completaron su procesamiento para ese cliente
- Los Aggregators pueden esperar a recibir los `SUM_AMOUNT` EOFs antes de computar el top

#### Implementación

La coordinación se implementa mediante dos recursos de comunicación:

| Recurso | Propósito | Tipo |
|--------|-----------|------|
| `INPUT_QUEUE` | Recibe datos y EOF del cliente | Queue (Round Robin) |
| `SUM_CONTROL_EXCHANGE` | Broadcast de EOF entre Sums | Exchange (Fanout) |

---

## 2. Escalabilidad con Clientes

El sistema soporta **múltiples clientes concurrentes** sin conflictos:

### Multiplexación por cliente_id

- Cada cliente recibe un `client_id` único asignado por el Gateway
- Este identificador fluye por todo el pipeline (Sum → Aggregation → Join → Gateway → Client)
- Los datos de diferentes clientes se procesan **independientemente** en paralelo

### Ventajas

- **Aislamiento**: datos de clientes no se mezclan
- **Concurrencia**: múltiples clientes pueden estar en diferentes etapas del procesamiento simultáneamente
- **Escalabilidad lineal**: el sistema mantiene O(1) overhead por cliente adicional

---

## 3. Escalabilidad con Instancias de Controles

### Sum (escalabilidad horizontal)

| Instancias | Patrón | Beneficio |
|---|---|---|
| 1 | Procesa todos los datos secuencialmente | Simplicidad |
| N > 1 | Distribución Round Robin por RabbitMQ + sincronización por control_exchange | ↑ Throughput, ↓ Latencia |

**Mecanismo**: Cada Sum procesa un subset de clientes (según RabbitMQ), sincroniza EOF mediante broadcast a través del control exchange.

### Aggregation (escalabilidad horizontal)

| Instancias | Patrón | Beneficio |
|---|---|---|
| 1 | Un Aggregator recibe todas las frutas de todos los clientes | Simplicidad |
| N > 1 | Cada Aggregator recibe solo frutas filtradas por `hash(cliente) + hash(fruta) % AGGREGATION_AMOUNT` | ↑ Throughput, ↓ Latencia |

**Mecanismo**: Cada Aggregator escucha un exchange específico identificado por su ID (`AGGREGATION_PREFIX_{ID}`). Sum ruatea frutas basándose en hash, evitando duplicación.

### Control de Graceful Shutdown

Cada instancia maneja **SIGTERM de manera independiente**:

```python
signal.signal(signal.SIGTERM, self.handle_sigterm)
```

1. Al recibir SIGTERM, la instancia marca `self.closed = True`
2. Deja de procesar nuevos mensajes
3. Cierra recursos (queues, exchanges, canales)
4. No afecta a otras instancias (cada una tiene sus propios recursos)

**Resultado**: Escalabilidad sin punto único de fallo en el shutdown.

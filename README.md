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
    fruit_hash = sum(ord(c) for c in fruit)
    indice_aggregation = (client_id + fruit_hash) % AGGREGATION_AMOUNT
```

Esto garantiza que:
- Una misma fruta de un cliente siempre se envía al mismo Aggregator
- Los datos se distribuyen uniformemente entre Aggregators
- No hay procesamiento redundante en Aggregation (cada fruta de cada cliente va a exactamente un Aggregator)

### Sincronización de EOF

**Múltiples instancias de Sum** requieren sincronización para garantizar que todos los datos lleguen antes de computar el top:

1. Cada Sum recibe datos de la cola `INPUT_QUEUE` (distribución Round Robin por RabbitMQ)
2. Cuando un Sum detecta EOF de un cliente, realiza dos acciones:
   - **Envía datos** a todos los Aggregators (incluidos EOF)
   - **Notifica** a otros Sums mediante un exchange de control (`SUM_CONTROL_EXCHANGE`)

3. Otros Sums reciben esta notificación y evitan procesar el mismo cliente nuevamente mediante `processed_clients`

**Resultado**: Cada Aggregator recibe exactamente `SUM_AMOUNT` mensajes EOF por cliente, garantizando que espere a que todos los Sums hayan terminado antes de computar el top.

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
| N > 1 | Cada Aggregator recibe solo frutas filtradas por `hash(fruta) % AGGREGATION_AMOUNT` | ↑ Throughput, ↓ Latencia |

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

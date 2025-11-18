"""
taller4_concurrencia.py

Taller 4 - Patrones Concurrentes
Autor: (tu nombre)
Fecha: (dd/mm/aaaa)

Implementa un sistema productor-consumidor en dos versiones:
 - hilos con threading.Thread
 - ThreadPoolExecutor

Ejecución:
    python taller4_concurrencia.py --mode threads
    python taller4_concurrencia.py --mode pool

Opciones:
    --producers N    # número de productores (por defecto 2)
    --consumers M    # número de consumidores (por defecto 3)
    --items K        # items por productor (por defecto 20)
    --maxsize S      # tamaño máximo de la cola (por defecto 5)
    --mode MODE      # 'threads' o 'pool' (por defecto 'threads')
"""

import threading
import queue
import time
import random
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed

# ---------- Parámetros por defecto (modificables por CLI) ----------
DEFAULT_PRODUCERS = 2
DEFAULT_CONSUMERS = 3
DEFAULT_ITEMS_PER_PRODUCER = 20
DEFAULT_QUEUE_MAXSIZE = 5

# ---------- Contadores protegidos ----------
class Counters:
    def __init__(self):
        self.produced = 0
        self.consumed = 0
        self.lock = threading.Lock()

    def inc_produced(self, n=1):
        with self.lock:
            self.produced += n

    def inc_consumed(self, n=1):
        with self.lock:
            self.consumed += n

# ---------- Funciones auxiliares ----------
def ts(start):
    """Timestamp relativo al inicio 'start' en segundos con 2 decimales."""
    return f"{(time.time() - start):.2f}s"

# ---------- Productor y Consumidor (para versión hilos) ----------
def productor(id_prod, cola, items, counters, start_time):
    """
    Genera 'items' elementos y los coloca en 'cola'.
    Imprime inicio, cada producción y finalización con timestamp relativo.
    """
    name = f"Productor-{id_prod}"
    print(f"[INICIO] {name}: inicio en t={ts(start_time)}")
    for i in range(1, items + 1):
        try:
            # Simular trabajo/espera de E/S
            time.sleep(random.uniform(0.05, 0.3))
            item = f"{name}-item-{i}"
            cola.put(item)  # bloquea si la cola está llena
            counters.inc_produced()
            print(f"[PRODUCE] {name}: producido {item} en t={ts(start_time)} (cola={cola.qsize()}/{cola.maxsize})")
        except Exception as e:
            # No queremos que un error detenga al productor sin trazabilidad
            print(f"[ERROR] {name}: excepción durante producción: {e}")
    print(f"[FIN] {name}: finaliza en t={ts(start_time)}")

def consumidor(id_cons, cola, stop_event, counters, start_time):
    """
    Consume elementos de 'cola' hasta que stop_event esté activado
    y la cola esté vacía. Maneja queue.Empty y registra trazas.
    """
    name = f"Consumidor-{id_cons}"
    print(f"[INICIO] {name}: inicio en t={ts(start_time)}")
    while True:
        # Si ya se ha marcado el evento y la cola está vacía, salimos
        if stop_event.is_set() and cola.empty():
            break
        try:
            # Intentar obtener un elemento con timeout para poder verificar stop_event regularmente
            item = cola.get(timeout=0.5)
        except queue.Empty:
            # No hay elemento en el intervalo de espera: revisamos si debemos terminar
            continue
        try:
            print(f"[CONSUME] {name}: obtiene {item} en t={ts(start_time)} (cola={cola.qsize()}/{cola.maxsize})")
            # Simular procesamiento
            time.sleep(random.uniform(0.1, 0.5))
            counters.inc_consumed()
            print(f"[PROCESADO] {name}: procesado {item} en t={ts(start_time)}")
        except Exception as e:
            print(f"[ERROR] {name}: excepción procesando {item}: {e}")
        finally:
            # Indicar que el elemento fue procesado
            try:
                cola.task_done()
            except Exception:
                pass
    print(f"[FIN] {name}: finaliza en t={ts(start_time)}")

# ---------- Version ThreadPoolExecutor ----------
def productor_pool(id_prod, cola, items, start_time):
    """
    Productor version pool: genera items y los pone en la cola.
    Retorna el número de items producidos (útil para verificación).
    """
    name = f"Productor-{id_prod}"
    produced_local = 0
    print(f"[INICIO_POOL] {name}: inicio en t={ts(start_time)}")
    for i in range(1, items + 1):
        time.sleep(random.uniform(0.05, 0.3))
        item = f"{name}-item-{i}"
        cola.put(item)
        produced_local += 1
        print(f"[PRODUCE_POOL] {name}: producido {item} en t={ts(start_time)} (cola={cola.qsize()}/{cola.maxsize})")
    print(f"[FIN_POOL] {name}: finaliza en t={ts(start_time)}")
    return produced_local

def consumidor_pool_worker(id_cons, cola, stop_event, counters, start_time):
    """
    Consumidor para ejecutar dentro de un ThreadPoolExecutor.
    Se comporta como consumidor de hilo largo: sale cuando stop_event + cola vacía.
    """
    consumidor(id_cons, cola, stop_event, counters, start_time)
    # consumidor ya maneja contadores via counters.inc_consumed

# ---------- Main: versión con hilos ----------
def main_threads(num_producers, num_consumers, items_per_producer, maxsize):
    """
    Implementación usando threading.Thread.
    """
    start_time = time.time()
    cola = queue.Queue(maxsize=maxsize)
    counters = Counters()
    stop_event = threading.Event()

    # Crear y lanzar productores
    productores = []
    for i in range(1, num_producers + 1):
        t = threading.Thread(target=productor, args=(i, cola, items_per_producer, counters, start_time), name=f"Productor-{i}")
        productores.append(t)
        t.start()

    # Crear y lanzar consumidores
    consumidores = []
    for j in range(1, num_consumers + 1):
        t = threading.Thread(target=consumidor, args=(j, cola, stop_event, counters, start_time), name=f"Consumidor-{j}")
        consumidores.append(t)
        t.start()

    # Esperar a que productores terminen
    for t in productores:
        t.join()

    # Esperar a que todos los items sean procesados
    cola.join()  # bloquea hasta que task_done() haya sido llamado para cada item puesto

    # Señalamos a consumidores que no llegará más producción
    stop_event.set()

    # Esperar a que consumidores terminen
    for t in consumidores:
        t.join()

    end_time = time.time()
    tiempo_total = end_time - start_time

    # Resultados
    print("\n" + "="*60)
    print("RESULTADOS (hilos):")
    print(f"Total producidos: {counters.produced}")
    print(f"Total consumidos: {counters.consumed}")
    print(f"Tiempo total: {tiempo_total:.2f} segundos")
    if counters.consumed > 0:
        print(f"Throughput (consumidos / s): {counters.consumed / tiempo_total:.2f}")
    print("="*60 + "\n")

# ---------- Main: versión con ThreadPoolExecutor ----------
def main_threadpool(num_producers, num_consumers, items_per_producer, maxsize):
    """
    Implementación usando ThreadPoolExecutor tanto para productores como para consumidores.
    Creamos tareas de productores en el pool y tareas 'consumidor' que permanecen activas.
    """
    start_time = time.time()
    cola = queue.Queue(maxsize=maxsize)
    counters = Counters()
    stop_event = threading.Event()

    # En el pool lanzamos:
    # - num_producers tareas de productor_pool
    # - num_consumers tareas de consumidor_pool_worker (llevan a cabo el loop de consumidor)
    total_tasks = num_producers + num_consumers
    max_workers = max(1, total_tasks)

    print(f"[INFO] Lanzando ThreadPoolExecutor con max_workers={max_workers} en t={ts(start_time)}")
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Subir consumidores de larga duración al pool
        consumer_futures = []
        for j in range(1, num_consumers + 1):
            future = executor.submit(consumidor_pool_worker, j, cola, stop_event, counters, start_time)
            consumer_futures.append(future)

        # Subir productores (retornan número producidos)
        prod_futures = []
        for i in range(1, num_producers + 1):
            future = executor.submit(productor_pool, i, cola, items_per_producer, start_time)
            prod_futures.append(future)

        # Esperar a que todos los productores terminen y acumular total producido
        produced_sum = 0
        for fut in as_completed(prod_futures):
            try:
                produced_local = fut.result()
                produced_sum += produced_local
                # actualizar contadores de forma segura
                counters.inc_produced(produced_local)
            except Exception as e:
                print(f"[ERROR_POOL] productor excepción: {e}")

        # Todos los productores finalizaron; esperar a que la cola se vacíe
        cola.join()
        # Señalamos a consumidores que ya no habrá más producción
        stop_event.set()

        # Ahora esperamos a que las tareas consumidoras terminen; como las tareas consumidoras
        # usan un bucle que termina al detectar stop_event y cola.empty() se acabarán.
        for fut in consumer_futures:
            try:
                # Cada futura ya terminó porque la función consumidor_pool_worker
                # llama a consumidor(...) y retorna. En caso de error, result() propagará.
                fut.result(timeout=30)
            except Exception as e:
                print(f"[ERROR_POOL] consumidor excepción o timeout: {e}")

    end_time = time.time()
    tiempo_total = end_time - start_time

    # Resultados
    print("\n" + "="*60)
    print("RESULTADOS (ThreadPoolExecutor):")
    print(f"Total producidos: {counters.produced} (suma verificada por productores: {produced_sum})")
    print(f"Total consumidos: {counters.consumed}")
    print(f"Tiempo total: {tiempo_total:.2f} segundos")
    if counters.consumed > 0:
        print(f"Throughput (consumidos / s): {counters.consumed / tiempo_total:.2f}")
    print("="*60 + "\n")

# ---------- Entrypoint ----------
def main():
    parser = argparse.ArgumentParser(description="Taller 4 - Productor/Consumidor (hilos vs ThreadPoolExecutor)")
    parser.add_argument("--producers", type=int, default=DEFAULT_PRODUCERS, help="Número de productores")
    parser.add_argument("--consumers", type=int, default=DEFAULT_CONSUMERS, help="Número de consumidores")
    parser.add_argument("--items", type=int, default=DEFAULT_ITEMS_PER_PRODUCER, help="Items por productor")
    parser.add_argument("--maxsize", type=int, default=DEFAULT_QUEUE_MAXSIZE, help="Tamaño máximo de la cola")
    parser.add_argument("--mode", type=str, choices=["threads", "pool"], default="threads", help="Modo: 'threads' o 'pool'")
    args = parser.parse_args()

    # Parámetros seleccionados
    num_producers = max(1, args.producers)
    num_consumers = max(1, args.consumers)
    items_per_producer = max(1, args.items)
    maxsize = max(1, args.maxsize)
    mode = args.mode

    header = f"Modo={mode} | productores={num_producers} | consumidores={num_consumers} | items={items_per_producer} | cola_max={maxsize}"
    print("="*len(header))
    print(header)
    print("="*len(header))

    if mode == "threads":
        main_threads(num_producers, num_consumers, items_per_producer, maxsize)
    else:
        main_threadpool(num_producers, num_consumers, items_per_producer, maxsize)

if __name__ == "__main__":
    main()

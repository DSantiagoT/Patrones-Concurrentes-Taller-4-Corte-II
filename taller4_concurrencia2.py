"""
taller_concurrencia2.py

Taller 4 - Patrones Concurrentes
Versión: ThreadPoolExecutor
Autor: (tu nombre)
Fecha: (dd/mm/aaaa)
"""

import concurrent.futures
import queue
import time
import random
import threading

# ---------- Contadores seguros ----------
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

# ---------- Función para timestamp relativo ----------
def ts(start):
    return f"{(time.time() - start):.2f}s"

# ---------- Productor ----------
def productor(id_prod, cola, items, counters, start_time):
    """Genera elementos y los coloca en la cola compartida."""
    name = f"Productor-{id_prod}"
    print(f"[INICIO] {name}: inicio en t={ts(start_time)}")
    for i in range(1, items + 1):
        time.sleep(random.uniform(0.05, 0.3))
        item = f"{name}-item-{i}"
        cola.put(item)
        counters.inc_produced()
        print(f"[PRODUCE] {name}: producido {item} en t={ts(start_time)} (cola={cola.qsize()}/{cola.maxsize})")
    print(f"[FIN] {name}: finaliza en t={ts(start_time)}")

# ---------- Consumidor ----------
def consumidor(id_cons, cola, stop_event, counters, start_time):
    """Extrae elementos de la cola y los procesa."""
    name = f"Consumidor-{id_cons}"
    print(f"[INICIO] {name}: inicio en t={ts(start_time)}")
    while True:
        if stop_event.is_set() and cola.empty():
            break
        try:
            item = cola.get(timeout=0.5)
        except queue.Empty:
            continue
        time.sleep(random.uniform(0.1, 0.5))
        counters.inc_consumed()
        print(f"[PROCESADO] {name}: procesado {item} en t={ts(start_time)} (cola={cola.qsize()}/{cola.maxsize})")
        cola.task_done()
    print(f"[FIN] {name}: finaliza en t={ts(start_time)}")

# ---------- Main ----------
def main():
    start_time = time.time()
    num_producers = 2
    num_consumers = 3
    items_per_producer = 20
    maxsize = 5

    cola = queue.Queue(maxsize=maxsize)
    counters = Counters()
    stop_event = threading.Event()

    # ---------- ThreadPoolExecutor ----------
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_producers + num_consumers) as executor:
        # Lanzar productores
        futures_prod = [executor.submit(productor, i, cola, items_per_producer, counters, start_time)
                        for i in range(1, num_producers + 1)]
        # Lanzar consumidores
        futures_cons = [executor.submit(consumidor, j, cola, stop_event, counters, start_time)
                        for j in range(1, num_consumers + 1)]

        # Esperar que todos los productores terminen
        concurrent.futures.wait(futures_prod)
        # Esperar que se procesen todos los elementos
        cola.join()
        stop_event.set()  # señal a consumidores para finalizar
        # Esperar que todos los consumidores terminen
        concurrent.futures.wait(futures_cons)

    end_time = time.time()
    print("\n" + "="*50)
    print("RESULTADOS (ThreadPoolExecutor):")
    print(f"Total producidos: {counters.produced}")
    print(f"Total consumidos: {counters.consumed}")
    print(f"Tiempo total de ejecución: {end_time - start_time:.2f} segundos")
    throughput = counters.consumed / (end_time - start_time)
    print(f"Throughput (consumidos / s): {throughput:.2f}")
    print("="*50)

if __name__ == "__main__":
    main()

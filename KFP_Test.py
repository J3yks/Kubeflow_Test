from kfp import compiler
from kfp.dsl import pipeline, component


@component(base_image="python:3.11-slim")
def cpu_burner(name: str, sec: int, totsec: str ="0",fail_prob:float=0.3) -> str:
    """Task CPU-bound che gira per N secondi con possibilita' di fallimento."""
    import time, math, os, random, sys
    totsec_int = int(totsec)
    start = time.time()
    while time.time() - start < sec:
        _ = [math.sqrt(i) for i in range(20_000)]
    msg = f"[{name}] Done after {sec}s, pid={os.getpid()}"
    print(msg)
    res=totsec_int+sec
    if random.random() < fail_prob:
        print(f"[{name}] Simulated failure!")
        sys.exit(1)  #uscita forzata -> fallimento nell'esecuzione
    return str(res) 

   
@component(base_image="python:3.11-slim")
def long_sleep(name: str, previous_output: str, sec: int) -> str:
    """Task sleeper che prende l'output del task precedente."""
    import time, os
    print(f"[{name}] Received input from previous task: {previous_output}")
    for i in range(sec // 10):
        print(f"[{name}] Slept {i*10}/{sec}s, pid={os.getpid()}")
        time.sleep(10)
    return f"slept {sec}s after receiving: {previous_output}"


@pipeline(
    name="myPipeline",
    description="Pipeline lineare dove ogni task prende in input l'output del precedente."
)
def my_pipeline(seconds: int=60):
    # Worker 1
    w1 = cpu_burner(name="worker-1", sec=seconds,totsec = "0")
    w1.set_retry(num_retries=3,    #retry in caso di fallimento
    backoff_duration="30s",        # tempo minimo tra retry
    backoff_factor=2.0   )         #fattore moltiplicativo per calcolo backoff
    w1.set_caching_options(False)

    # Worker 2 prende l'output di w1
    w2 = cpu_burner(name="worker-2", sec=seconds, totsec =w1.output)
    w2.set_retry(num_retries=3,
    backoff_duration="30s",        
    backoff_factor=2.0   )
    w2.set_caching_options(False)

    # Worker 3 prende l'output di w2
    w3 = cpu_burner(name="worker-3", sec=seconds, totsec=w2.output)
    w3.set_retry(num_retries=3,
    backoff_duration="30s",       
    backoff_factor=2.0   )
    w3.set_caching_options(False)

    # Sleeper prende l'output di w3
    s = long_sleep(name="long-sleeper", previous_output=w3.output, sec=seconds)
    s.set_retry(num_retries=3,
    backoff_duration="30s",       
    backoff_factor=2.0   )
    s.set_caching_options(False)

if __name__ == "__main__":
    compiler.Compiler().compile(
        pipeline_func=my_pipeline,
        package_path="testingPipeline.yaml"
    )
    print("READY: testingPipeline.yaml")

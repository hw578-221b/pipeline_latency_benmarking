import pipeline_test as pipeline

# test runtime (s), generation gap (us)
conditions = [
    (5, 500),
    (5, 1000),
    (5, 5000)
]

def main():
    for index, (runtime, gap) in enumerate(conditions):
        print("====================================================")
        print(f"Test {index+1}, runtime: {runtime} s")
        print(f"Running pipeline with generation gap: {gap} us...")
        print("====================================================")
        pipeline.run_pipeline(index+1, runtime, gap, "other", 0, "shm", False, False, f"event_log{index+1}.txt", True)
        print("")


if __name__ == "__main__":
    main()
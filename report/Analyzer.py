from pprint import pprint
import statistics
import matplotlib.pyplot as plt


def read_file(fileName: str) -> list[list[str]]:
    with open(fileName, "r") as f:
        lines = [[
                tok.replace("[1;32m", "").replace("[0m", "").strip()
                for tok in line.split("\t") 
                if tok != ""
            ]
            for line in f.read().strip().split("\n") 
            if line != ""
        ]
    return lines

def parse_file_phase1(lines):
    parsed_data = dict()
    curr_conf   = None
    
    for line in lines:
        if not line[0].startswith("Case"): continue
        parsed_data[line[1]] = dict()
    
    for line in lines:
        if line[0].startswith("TESTCASE"):
            curr_conf = line[0].split(": ")[1]
        elif line[0].startswith("Case"):
            performance = line[2].split()
            parsed_data[line[1]][curr_conf] = {
                "Benchmark(MQPS)": performance[1][:-4],
                "RunTime(ms)"    : performance[3][:-3],
                "PrepTime(ms)"   : performance[6][:-3],
                "DelTime(ms)"    : performance[9]
            }
        else:
            continue
    return parsed_data

def parse_file_phase2(result):
    phase2_result = dict()
    for case_name in result:
        if case_name in phase2_result: continue
        phase2_result[case_name] = dict()
    
    for case_name in result:
        for case_key in result[case_name]:
            case_data = result[case_name][case_key]
            tree_type, num_thread = case_key.split(" x")
            num_thread = int(num_thread)
            
            if tree_type not in phase2_result[case_name]:
                phase2_result[case_name][tree_type] = {
                    "numThread"         : [],
                    "Benchmark(MQPS)"   : [],
                    "RunTime(ms)"       : [],
                    "PrepTime(ms)"      : [],
                    "DelTime(ms)"       : [],
                }
            phase2_result[case_name][tree_type]["numThread"].append(num_thread)
            phase2_result[case_name][tree_type]["Benchmark(MQPS)"].append(float(case_data["Benchmark(MQPS)"]))
            phase2_result[case_name][tree_type]["RunTime(ms)"].append(float(case_data["RunTime(ms)"]))
            phase2_result[case_name][tree_type]["PrepTime(ms)"].append(float(case_data["PrepTime(ms)"]))
            phase2_result[case_name][tree_type]["DelTime(ms)"].append(float(case_data["DelTime(ms)"]))
    
    return phase2_result

def parse_file_phase3(result, groupSubstr, tree_type, threads=[1, 2, 4, 6, 8]):
    selected_cases = []
    for case_name in result:
        if groupSubstr not in case_name: continue
        selected_cases.append(result[case_name][tree_type])
    return statCases(selected_cases, threads)

def statCases(cases, threads=[1, 2, 4, 6, 8]):
    raw_key = ["Benchmark(MQPS)", "RunTime(ms)", "PrepTime(ms)", "DelTime(ms)"]
    stat_result = {
                    "numThread"          : threads,
                    "raw_Benchmark(MQPS)": [[] for _ in range(len(threads))],
                    "std_Benchmark(MQPS)": [-1 for _ in range(len(threads))],
                    "avg_Benchmark(MQPS)": [-1 for _ in range(len(threads))],
                    "raw_RunTime(ms)"    : [[] for _ in range(len(threads))],
                    "std_RunTime(ms)"    : [-1 for _ in range(len(threads))],
                    "avg_RunTime(ms)"    : [-1 for _ in range(len(threads))],
                    "raw_PrepTime(ms)"   : [[] for _ in range(len(threads))],
                    "std_PrepTime(ms)"   : [-1 for _ in range(len(threads))],
                    "avg_PrepTime(ms)"   : [-1 for _ in range(len(threads))],
                    "raw_DelTime(ms)"    : [[] for _ in range(len(threads))],
                    "std_DelTime(ms)"    : [-1 for _ in range(len(threads))],
                    "avg_DelTime(ms)"    : [-1 for _ in range(len(threads))],
                }
    for case in cases:
        for key in raw_key:
            for idx in range(len(threads)):
                stat_result["raw_" + key][idx].append(case[key][idx])
    
    for key in raw_key:
        for idx in range(len(threads)):
            stat_result["avg_" + key][idx] = statistics.mean(stat_result["raw_" + key][idx])
            stat_result["std_" + key][idx] = statistics.stdev(stat_result["raw_" + key][idx])
        
    return stat_result

def plotMQPS(result, caseSubstr, tree_types, save_to, ticks=[1, 2, 4, 6, 8], color=['#1f77b4','#17becf','#2ca02c','#9467bd', '#7f7f7f']):
    # local_result = result[testcaseName]
    plt.figure(figsize=(10, 6), dpi=360)
    
    # plot baseline first
    baseline_stat = parse_file_phase3(result, caseSubstr, "Baseline", threads=[1])
    baseline_clr  = color[-1]
    baselineMQPS  = baseline_stat["avg_Benchmark(MQPS)"][0]
    baselineMQPS_std = baseline_stat["std_Benchmark(MQPS)"][0]
    
    plt.axhline(y=baselineMQPS, color=baseline_clr, linestyle="--")
    plt.text(
        ticks[-2], baselineMQPS, 'Baseline (Sequential)', 
        verticalalignment='top', fontsize=12, color=baseline_clr,
    )
    plt.fill_between(ticks, baselineMQPS-baselineMQPS_std, baselineMQPS+baselineMQPS_std, color=baseline_clr, alpha=.15)
    
    for i, tree_type in enumerate(tree_types):
        selected_cases = parse_file_phase3(result, caseSubstr, tree_type, ticks)
        benchmarkMQPS = selected_cases["avg_Benchmark(MQPS)"]
        benchmarkMQPS_up   = [b+s for b, s in zip(benchmarkMQPS, selected_cases["std_Benchmark(MQPS)"])]
        benchmarkMQPS_down = [b-s for b, s in zip(benchmarkMQPS, selected_cases["std_Benchmark(MQPS)"])]
        plt.plot(ticks, benchmarkMQPS, label=tree_type, color=color[i])
        plt.fill_between(ticks, benchmarkMQPS_down, benchmarkMQPS_up, color=color[i], alpha=.15)
        
    plt.grid(True, which='both', linestyle='--', linewidth=0.5)
    plt.legend()
    plt.savefig(save_to)
    plt.close()


result = parse_file_phase2(parse_file_phase1(read_file("Benchmark_prefill0.txt")))
plotMQPS(result, "B_megaGet", ["CoarseGrain", "FineGrain", "LockFree"], "prefill0_get_stat.jpg")
plotMQPS(result, "B_megaMix", ["CoarseGrain", "FineGrain", "LockFree"], "prefill0_mix_stat.jpg")

result = parse_file_phase2(parse_file_phase1(read_file("Benchmark_prefill0_ord9.txt")))
plotMQPS(result, "B_megaGet", ["CoarseGrain", "FineGrain", "LockFree"], "prefill0_get_stat_ord9.jpg")
plotMQPS(result, "B_megaMix", ["CoarseGrain", "FineGrain", "LockFree"], "prefill0_mix_stat_ord9.jpg")
    
result = parse_file_phase2(parse_file_phase1(read_file("Benchmark_prefill1e6.txt")))
plotMQPS(result, "B_megaGet", ["CoarseGrain", "FineGrain", "LockFree"], "prefill1e6_get_stat.jpg")
plotMQPS(result, "B_megaMix", ["CoarseGrain", "FineGrain", "LockFree"], "prefill1e6_mix_stat.jpg")

result = parse_file_phase2(parse_file_phase1(read_file("Benchmark_prefill1e6_ord9.txt")))
plotMQPS(result, "B_megaGet", ["CoarseGrain", "FineGrain", "LockFree"], "prefill1e6_get_stat_ord9.jpg")
plotMQPS(result, "B_megaMix", ["CoarseGrain", "FineGrain", "LockFree"], "prefill1e6_mix_stat_ord9.jpg")


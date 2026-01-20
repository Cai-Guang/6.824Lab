#!/usr/bin/env python3
import sys
import os
import subprocess
import argparse
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

# ANSI 颜色代码，用于美化输出
class Colors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'

# 全局锁，用于打印输出防止错乱
print_lock = Lock()

def run_test(iteration, test_pattern, log_dir):
    """
    运行单个测试任务
    """
    log_file = os.path.join(log_dir, f"test_{iteration}.log")
    
    # 构造命令：开启 Race 检测，-v 输出详细日志以便失败时查看
    cmd = ["go", "test", "-run", test_pattern, "-race", "-v"]
    
    start_time = time.time()
    
    # 执行命令
    try:
        # 捕获 stdout 和 stderr
        result = subprocess.run(
            cmd, 
            stdout=subprocess.PIPE, 
            stderr=subprocess.STDOUT, 
            text=True, 
            timeout=200 # 单个测试超时时间，防止死锁卡住
        )
        duration = time.time() - start_time
        
        if result.returncode != 0:
            # 如果失败，写入日志文件
            with open(log_file, "w") as f:
                f.write(result.stdout)
            return False, duration, iteration, "Failed"
        else:
            # 如果成功，不需要保留日志（为了节省空间）
            return True, duration, iteration, "Passed"

    except subprocess.TimeoutExpired:
        # 超时处理
        with open(log_file, "w") as f:
            f.write(f"Test timed out after 120 seconds.")
        return False, 120.0, iteration, "Timeout"
    except Exception as e:
        return False, 0.0, iteration, str(e)

def main():
    parser = argparse.ArgumentParser(description="MIT 6.824 Raft Stress Tester")
    parser.add_argument("-n", "--runs", type=int, default=100, help="Total number of runs (default: 100)")
    parser.add_argument("-c", "--concurrency", type=int, default=5, help="Number of concurrent workers (default: 30)")
    parser.add_argument("-t", "--test", type=str, default="3C", help="Test pattern regex (default: 3C)")
    args = parser.parse_args()

    # 创建日志目录
    log_dir = "stress_logs"
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    print(f"{Colors.HEADER}Starting Stress Test...{Colors.ENDC}")
    print(f"Target: {Colors.OKBLUE}Test{args.test}{Colors.ENDC}")
    print(f"Total Runs: {args.runs} | Concurrency: {args.concurrency}")
    print("-" * 50)

    passed = 0
    failed = 0
    futures = []

    start_total_time = time.time()

    # 使用线程池进行并发执行
    with ThreadPoolExecutor(max_workers=args.concurrency) as executor:
        for i in range(1, args.runs + 1):
            futures.append(executor.submit(run_test, i, args.test, log_dir))

        # 处理结果
        for future in as_completed(futures):
            is_success, duration, iter_id, msg = future.result()
            
            with print_lock:
                if is_success:
                    passed += 1
                    status = f"{Colors.OKGREEN}PASS{Colors.ENDC}"
                    # 成功时不打印详细信息，保持清爽，或者可以选择打印简略信息
                    # print(f"Run {iter_id:3d}: {status} ({duration:.2f}s)")
                else:
                    failed += 1
                    status = f"{Colors.FAIL}FAIL{Colors.ENDC}"
                    print(f"Run {iter_id:3d}: {status} ({duration:.2f}s) - Log: {log_dir}/test_{iter_id}.log")

                # 动态刷新进度条 (覆盖同一行)
                sys.stdout.write(f"\rProgress: {passed + failed}/{args.runs} | Passed: {Colors.OKGREEN}{passed}{Colors.ENDC} | Failed: {Colors.FAIL}{failed}{Colors.ENDC}")
                sys.stdout.flush()

    total_time = time.time() - start_total_time
    print("\n" + "-" * 50)
    
    # 最终汇总
    if failed == 0:
        print(f"{Colors.OKGREEN}ALL TESTS PASSED!{Colors.ENDC}")
    else:
        print(f"{Colors.FAIL}{failed} TESTS FAILED.{Colors.ENDC} Check {log_dir}/ for details.")
    
    print(f"Total Time: {total_time:.2f}s")

if __name__ == "__main__":
    main()
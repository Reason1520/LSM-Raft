#!/bin/bash

# 更详细的测试运行脚本 - 分别运行2A/2B/2C/2D各1000次

echo "开始运行Raft测试..."

# 创建结果目录
mkdir -p test_results

# 定义测试函数
run_detailed_test() {
    local test_pattern=$1
    local test_name=$2
    local count=$3
    
    echo "========================================"
    echo "开始运行 $test_name 测试 $count 次"
    echo "========================================"
    
    passed=0
    failed=0
    fail_log="test_results/${test_pattern}_failures.log"
    
    # 清空之前的失败日志
    > "$fail_log"
    
    for i in $(seq 1 $count); do
        printf "\r进度: $i/$count"
        output=$(go test -run "$test_pattern" -timeout 10s 2>&1)
        if [ $? -eq 0 ]; then
            ((passed++))
        else
            ((failed++))
            echo "失败 #$failed (总迭代 #$i):" >> "$fail_log"
            echo "$output" >> "$fail_log"
            echo "---" >> "$fail_log"
        fi
    done
    
    echo ""
    echo "----------------------------------------"
    echo "$test_name 结果:"
    echo "  成功: $passed"
    echo "  失败: $failed"
    echo "  总计: $count"
    echo "  成功率: $(echo "scale=2; $passed*100/$count" | bc)%"
    
    if [ $failed -gt 0 ]; then
        echo "  失败详情已保存至: $fail_log"
    fi
    echo "----------------------------------------"
    echo ""
}

# 运行各个测试
run_detailed_test "2A" "2A (初始选举)" 1000
run_detailed_test "2B" "2B (日志复制)" 1000
run_detailed_test "2C" "2C (持久化)" 1000
run_detailed_test "2D" "2D (快照)" 1000

echo "========================================"
echo "所有测试已完成! 详细结果保存在 test_results 目录中"
echo "========================================"
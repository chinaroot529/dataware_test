#!/bin/bash
#
# CDC系统一键启动脚本
# 自动化部署Debezium + Kafka + Doris的CDC流水线
#

set -e  # 遇到错误立即退出

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# 日志函数
log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

log_step() {
    echo -e "${BLUE}[STEP]${NC} $1"
}

# 检查依赖
check_dependencies() {
    log_step "检查系统依赖..."
    
    # 检查Docker
    if ! command -v docker &> /dev/null; then
        log_error "Docker未安装，请先安装Docker"
        exit 1
    fi
    
    # 检查Docker Compose
    if ! command -v docker-compose &> /dev/null; then
        log_error "Docker Compose未安装，请先安装Docker Compose"
        exit 1
    fi
    
    # 检查Python3
    if ! command -v python3 &> /dev/null; then
        log_error "Python3未安装，请先安装Python3"
        exit 1
    fi
    
    # 检查pip包
    if ! python3 -c "import pymysql, requests" &> /dev/null; then
        log_warn "缺少Python依赖包，正在安装..."
        pip3 install pymysql requests
    fi
    
    log_info "✅ 依赖检查完成"
}

# 检查端口占用
check_ports() {
    log_step "检查端口占用..."
    
    ports=(2181 9092 8081 8083 8080 8082 9090 3000)
    occupied_ports=()
    
    for port in "${ports[@]}"; do
        if lsof -Pi :$port -sTCP:LISTEN -t >/dev/null 2>&1; then
            occupied_ports+=($port)
        fi
    done
    
    if [ ${#occupied_ports[@]} -gt 0 ]; then
        log_warn "以下端口被占用: ${occupied_ports[*]}"
        read -p "是否继续？某些服务可能无法启动 (y/N): " -n 1 -r
        echo
        if [[ ! $REPLY =~ ^[Yy]$ ]]; then
            exit 1
        fi
    else
        log_info "✅ 端口检查完成"
    fi
}

# 启动CDC技术栈
start_cdc_stack() {
    log_step "启动CDC技术栈..."
    
    cd "$(dirname "$0")/.."
    
    # 启动Docker Compose
    docker-compose -f docker-compose.cdc.yml up -d
    
    log_info "📋 等待服务启动..."
    sleep 30
    
    # 等待服务就绪
    log_info "🔍 检查服务状态..."
    
    # 检查Kafka
    max_attempts=30
    attempt=0
    while [ $attempt -lt $max_attempts ]; do
        if curl -f -s http://localhost:8080 > /dev/null 2>&1; then
            log_info "✅ Kafka UI已启动"
            break
        fi
        sleep 5
        ((attempt++))
    done
    
    # 检查Kafka Connect
    attempt=0
    while [ $attempt -lt $max_attempts ]; do
        if curl -f -s http://localhost:8083 > /dev/null 2>&1; then
            log_info "✅ Kafka Connect已启动"
            break
        fi
        sleep 5
        ((attempt++))
    done
    
    log_info "✅ CDC技术栈启动完成"
}

# 部署MySQL连接器
deploy_connector() {
    log_step "部署MySQL CDC连接器..."
    
    # 等待Kafka Connect完全就绪
    sleep 10
    
    python3 scripts/cdc_manager.py deploy-connector
    
    if [ $? -eq 0 ]; then
        log_info "✅ MySQL CDC连接器部署成功"
    else
        log_error "❌ MySQL CDC连接器部署失败"
        return 1
    fi
}

# 设置Doris表和Routine Load
setup_doris() {
    log_step "设置Doris表和Routine Load..."
    
    # 创建表
    python3 scripts/setup_doris_routine_load.py create-table
    if [ $? -ne 0 ]; then
        log_error "❌ 创建Doris表失败"
        return 1
    fi
    
    # 等待一下再创建Routine Load
    sleep 5
    
    # 创建Routine Load任务
    python3 scripts/setup_doris_routine_load.py create-load
    if [ $? -eq 0 ]; then
        log_info "✅ Doris Routine Load设置成功"
    else
        log_error "❌ Doris Routine Load设置失败"
        return 1
    fi
}

# 运行测试
run_test() {
    log_step "运行CDC端到端测试..."
    
    # 等待系统稳定
    sleep 10
    
    python3 scripts/test_cdc_pipeline.py --test-type full
    
    if [ $? -eq 0 ]; then
        log_info "✅ CDC测试通过"
    else
        log_warn "⚠️  CDC测试部分失败，请检查日志"
    fi
}

# 显示服务信息
show_services() {
    log_step "CDC系统已启动完成！"
    echo
    echo "📋 服务访问地址："
    echo "   Kafka UI:      http://localhost:8080"
    echo "   Debezium UI:   http://localhost:8082"
    echo "   Grafana:       http://localhost:3000 (admin/admin)"
    echo "   Prometheus:    http://localhost:9090"
    echo
    echo "🔧 管理命令："
    echo "   查看状态:      python3 cdc/scripts/cdc_manager.py health"
    echo "   查看统计:      python3 cdc/scripts/cdc_manager.py stats"
    echo "   运行测试:      python3 cdc/scripts/test_cdc_pipeline.py"
    echo "   停止服务:      python3 cdc/scripts/cdc_manager.py stop"
    echo
}

# 清理函数
cleanup() {
    if [ $? -ne 0 ]; then
        log_error "启动过程中发生错误，正在清理..."
        docker-compose -f cdc/docker-compose.cdc.yml down 2>/dev/null || true
    fi
}

# 主函数
main() {
    echo "🚀 CDC系统一键启动脚本"
    echo "基于Debezium 3.2 + Kafka + Doris的实时数据同步"
    echo "=================================================="
    echo
    
    # 设置清理陷阱
    trap cleanup EXIT
    
    # 执行步骤
    check_dependencies
    check_ports
    start_cdc_stack
    
    # 部署连接器（可选）
    echo
    read -p "是否立即部署MySQL CDC连接器？(Y/n): " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Nn]$ ]]; then
        log_info "跳过连接器部署，可稍后手动执行："
        echo "   python3 cdc/scripts/cdc_manager.py deploy-connector"
    else
        deploy_connector
        
        # 设置Doris（可选）
        echo
        read -p "是否立即设置Doris表和Routine Load？(Y/n): " -n 1 -r
        echo
        if [[ $REPLY =~ ^[Nn]$ ]]; then
            log_info "跳过Doris设置，可稍后手动执行："
            echo "   python3 cdc/scripts/setup_doris_routine_load.py create-table"
            echo "   python3 cdc/scripts/setup_doris_routine_load.py create-load"
        else
            setup_doris
            
            # 运行测试（可选）
            echo
            read -p "是否运行端到端测试？(Y/n): " -n 1 -r
            echo
            if [[ ! $REPLY =~ ^[Nn]$ ]]; then
                run_test
            fi
        fi
    fi
    
    show_services
    
    log_info "🎉 CDC系统启动完成！"
}

# 参数处理
case "${1:-}" in
    "help"|"--help"|"-h")
        echo "用法: $0 [选项]"
        echo "选项:"
        echo "  help, --help, -h    显示此帮助信息"
        echo "  check              只检查依赖和端口"
        echo "  start              只启动服务，不部署"
        echo "  full               完整部署（默认）"
        exit 0
        ;;
    "check")
        check_dependencies
        check_ports
        exit 0
        ;;
    "start")
        check_dependencies
        start_cdc_stack
        show_services
        exit 0
        ;;
    "full"|"")
        main
        ;;
    *)
        log_error "未知选项: $1"
        echo "使用 '$0 help' 查看帮助"
        exit 1
        ;;
esac 
#!/usr/bin/env python3
"""
Workshop API 测试程序
测试 url-generate 后端的两个接口：
1. POST /generate - 流式生成 HTML
2. POST /upload - 上传文件到 OSS
"""

import requests
import sys
import time

BASE_URL = "http://localhost:5000"


def test_generate():
    """测试流式生成接口"""
    print("=" * 60)
    print("测试 1: 流式生成 HTML (/generate)")
    print("=" * 60)

    url = f"{BASE_URL}/generate"
    data = {
        "context": "帮我创建一个简单的蓝色按钮，点击时显示 'Hello World'",
        "system_prompt": "你是一个HTML生成专家"
    }

    print(f"请求 URL: {url}")
    print(f"请求数据: {data}")
    print("-" * 60)

    try:
        response = requests.post(url, json=data, stream=True, timeout=60)
        response.raise_for_status()

        print("✅ 连接成功，开始接收流式数据...\n")

        # 收集完整内容
        full_content = ""
        chunk_count = 0

        for chunk in response.iter_content(chunk_size=1024):
            if chunk:
                decoded = chunk.decode('utf-8')
                full_content += decoded
                chunk_count += 1
                # 实时显示前100字符，避免刷屏
                display = decoded[:100].replace('\n', ' ')
                print(f"[块{chunk_count:3d}] {display}...")

        print("\n" + "=" * 60)
        print(f"✅ 流式接收完成！共 {chunk_count} 个数据块")
        print(f"✅ 总长度: {len(full_content)} 字符")
        print("=" * 60)

        # 检查是否包含HTML
        if '<html' in full_content.lower() or '<!doctype' in full_content.lower():
            print("✅ 响应包含 HTML 标签")
        else:
            print("⚠️ 响应可能不包含完整 HTML")

        return full_content

    except requests.exceptions.ConnectionError:
        print(f"❌ 连接失败: 无法连接到 {BASE_URL}")
        print("   请确保后端服务已启动: docker compose up -d")
        return None
    except Exception as e:
        print(f"❌ 请求失败: {e}")
        return None


def test_upload(html_content):
    """测试上传接口"""
    print("\n" + "=" * 60)
    print("测试 2: 上传文件到 OSS (/upload)")
    print("=" * 60)

    if not html_content:
        print("❌ 没有 HTML 内容可上传，跳过此测试")
        return False

    url = f"{BASE_URL}/upload"
    filename = f"test_{int(time.time())}.html"

    print(f"请求 URL: {url}")
    print(f"文件名: {filename}")
    print(f"文件大小: {len(html_content)} 字符")
    print("-" * 60)

    try:
        # 准备文件
        files = {
            'file': (filename, html_content.encode('utf-8'), 'text/html')
        }

        response = requests.post(url, files=files, timeout=30)
        response.raise_for_status()

        result = response.json()
        print(f"✅ 上传成功！")
        print(f"✅ 返回 URL: {result.get('url', 'N/A')}")

        # 尝试访问 URL
        file_url = result.get('url')
        if file_url:
            print(f"\n正在验证文件可访问性...")
            time.sleep(1)  # 等待 OSS 同步
            check_response = requests.head(file_url, timeout=10)
            if check_response.status_code == 200:
                print(f"✅ 文件已可访问: {file_url}")
            else:
                print(f"⚠️ 文件可能还在同步中 (HTTP {check_response.status_code})")

        return True

    except requests.exceptions.ConnectionError:
        print(f"❌ 连接失败: 无法连接到 {BASE_URL}")
        return False
    except Exception as e:
        print(f"❌ 上传失败: {e}")
        if hasattr(e, 'response') and e.response:
            print(f"   错误详情: {e.response.text}")
        return False


def main():
    print("\n" + "=" * 60)
    print(" Workshop API 测试程序")
    print(f" 后端地址: {BASE_URL}")
    print("=" * 60 + "\n")

    # 检查后端是否在线
    try:
        health = requests.get(f"{BASE_URL}/docs", timeout=5)
        print(f"✅ 后端服务在线 (HTTP {health.status_code})\n")
    except:
        print(f"❌ 后端服务未响应，请检查:")
        print(f"   1. Docker 容器是否运行: docker ps")
        print(f"   2. 端口是否正确: {BASE_URL}")
        print(f"   3. 启动命令: docker compose up -d\n")
        sys.exit(1)

    # 测试生成接口
    html_content = test_generate()

    if html_content:
        # 测试上传接口
        test_upload(html_content)

    print("\n" + "=" * 60)
    print(" 测试完成")
    print("=" * 60)


if __name__ == "__main__":
    main()

import json
import threading
import time
import cv2
import requests
import schedule
from loguru import logger






def video_check(url, mapping):
    """
    TODO 读取视频
    :param url:
    :param mapping:
    :return:
    """
    global success_sum, fail_sum  # 声明sum为全局变量
    success_sum, fail_sum = 0,0 # 初始默认统计成功的视频数量

    cap = cv2.VideoCapture(url, cv2.CAP_FFMPEG) #  首选项来强制后端ffmpeg
    cap.set(cv2.CAP_PROP_OPEN_TIMEOUT_MSEC, 5000)
    cap.set(cv2.CAP_PROP_N_THREADS, 0)

    if cap.isOpened():  # 打开视频
        ret, frame = cap.read()  # 读取视频，读取到的某一帧存储到frame，若是读取成功，ret为True，反之为False
        if ret:  # 读取到视频数据 返回True
            count = 0
            for i in range(5):  # 循环读取5帧图像数据
                ndarray = frame[0][i]
                if list(set(ndarray)) == [255] or list(set(ndarray)) == [0]:  # 判断5帧图像数据是否一致,全黑/全白为[255,255,255] [0,0,0]
                    count += 1
                    if count == 5:
                        logger.warning(f"[{mapping.get(url)['factoryName']}][{mapping.get(url)['name']}] 视频读取异常,不可正常播放")
                        fail_sum += 1  # 视频异常计数器加1
                else:
                    count += 1
                    if count == 5:
                        logger.info(f"[{mapping.get(url)['factoryName']}][{mapping.get(url)['name']}] 视频读取成功,可正常播放")
                        success_sum += 1  # 视频正常计数器加1
        else:  # 未读取到视频数据返回False
            logger.warning(f"[{mapping.get(url)['factoryName']}][{mapping.get(url)['name']}] 视频异常,设备不可以用或无法连接")
            fail_sum += 1  # 视频异常计数器加1
    else:
        logger.warning(f"[{mapping.get(url)['factoryName']}][{mapping.get(url)['name']}] 视频打开失败")
        fail_sum += 1  # 视频异常计数器加1
    cap.release()


#提取json配置的key
def extract_key_value(data,keyw):
    dict_data = {}
    # 如果是字典，检查是否有rtmpUrl键
    if isinstance(data, dict):
        for key, value in data.items():
            if key == keyw and value != "":
                dict_data[value] = {'name': data['name'], 'factoryName': data['factoryName']}
            else:
                # 递归搜索其他可能包含hlsUrl的字段
                dict_data.update(extract_key_value(value, keyw))  # 更新字典

    # 如果是列表，递归每一项
    elif isinstance(data, list):
        for item in data:
            dict_data.update(extract_key_value(item, keyw))  # 更新字典
    return dict_data



def video_check_thread(data, keyw):
    """
    TODO 启用多线程检查
    :param data:
    :param keyw:
    :return:
    """
    mapping = extract_key_value(data, "hlsUrl")
    threads = []
    for url in mapping.keys():
        thread = threading.Thread(target=video_check, args=(url, mapping))
        thread.start()  # 启动线程
        threads.append(thread)  # 加入线程池

    for thread in threads:
        thread.join()  # 等待当前线程结束，再执行下一个线程



# 发送企微消息
def send_msg(success_sum, fail_sum):
    total = success_sum + fail_sum
    temp = (success_sum / total) * 100
    passrate = "{:.2f}".format(temp)  # 格式化保留2位小数点
    url = "https://xxx/send?key=f218ffac-1d3e-4c7b-b1b8-168887855b78"
    headers = {"Content-Type": "application/json"}

    content = f'''**监控视频巡检报告**\n
                        >巡检时间: <font color="info">{time.strftime("%Y-%m-%d %X", time.localtime())}</font>\n
                        >巡检视频总数: <font color="info">{total}</font>个\n
                        >正常数量: <font color="info">{success_sum}</font>个\n
                        >异常数量: <font color="warning">{fail_sum}</font>个\n
                        >通过率: <font color="warning">{passrate}%</font>\n
                        > <font color="comment">请相关同学注意!!! </font>'''

    data = {
        "msgtype": "markdown",
        "markdown": {
            "content": content,
            "mentioned_list":["xx","@all"],
            "mentioned_mobile_list": ["@18976231206", "@all"]
            }
        }
    resp = requests.post(url=url, json=data, headers=headers)
    if resp:
        logger.info("巡检结果企微消息发送成功...")
    else:
        logger.error("巡检结果企微消息发送失败...")


# 定时任务调度
def send_job():
    with open("monitor.json", "r", encoding='utf8') as file:
        result = json.load(file)
        video_check_thread(result, "hlsUrl")
        send_msg(success_sum, fail_sum)





if __name__ == "__main__":
    # 定时每天某个时刻执行一次job函数
    schedule.every().day.at("09:06").do(send_job)
    while True:
        schedule.run_pending()
        time.sleep(600)



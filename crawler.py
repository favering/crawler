#! /usr/bin/env python3
# -*- coding: UTF-8 -*-

import sys
import queue
import urllib.request
import time
import re
from urllib.parse import urlparse, quote
import os
import logging
import logging.handlers
import copy
import threading
import sqlite3

# 日志对象
_logger = None
# sqlite读写对象
_sqlite_wrapper = None
# 页面请求timeout
_OPENURL_TIMEOUT = None

class ThreadPool:
    """
    线程池基类
    """
    def __init__(self, thread_num):
        self.threads = \
            [threading.Thread(target=self.thread_routing) for _ in range(thread_num)]

        # task_queue为PriorityQueue，
        # 主要是为区分常规任务和终止任务不同的优先级
        self.task_queue = queue.PriorityQueue()

    def thread_routing(self):
        """
        所有线程的执行函数
        :return:
        """
        pass

    def put_task(self, raw, prior=2):
        """
        添加任务到队列
        :param raw: 任务内容
        :param prior: 任务优先级。
                      常规任务的优先级设为 2，默认。
                      线程终止任务的优先级较高，设为 1， 即：收到终止命令，立即退出
        :return:
        """
        self.task_queue.put((prior, raw))

    def start(self):
        """
        开启所有线程
        :return:
        """
        for thread in self.threads:
            thread.start()

    def wait(self):
        """
        等待任务队列全部完成
        :return:
        """
        self.task_queue.join()
        self.stop()

    def stop(self):
        """
        终止线程池
        :return:
        """
        pass

class DownloadPool(ThreadPool):
    """
    网页爬取线程池
    """
    def __init__(self, thread_num, url, depth, no_cross_domain_except):
        super().__init__(thread_num)

        # 目标爬取深度
        self.depth = depth

        # 使url格式完整
        if urlparse(url).scheme == '':
            url = "http://" + url

        # 允许爬取任意域名
        if no_cross_domain_except is None:
            self.allowed_domain = None
        # 只允许爬取指定域名（默认包含初始url的域）
        else:
            self.allowed_domain = no_cross_domain_except + [urlparse(url).netloc]

        # 处理过的url链接
        self.prcessed_url = []

        # 线程池当前状态字典
        # qsize 当前队列中的任务数
        # stop 线程池是否进入终止状态(True:正在终止 False:未进入终止)
        self.task_status = {}
        self.task_status["qsize"] = 0
        self.task_status['stop'] = False

        # crawl_st 各线程爬取状态字典，添加到self.task_status
        # key           线程名
        # link_count    该线程处理的链接数
        # img_count     该线程下载的图片数
        # cur_depth     该线程当前深度
        # state         该线程的存活状态
        crawl_st = {}
        for thread in self.threads:
            crawl_st[thread.name] = \
                {'link_count': 0, 'img_count': 0, 'cur_depth': 0, 'state': 'dead'}
        self.task_status['crawl_st'] = crawl_st

        # self.task_status读写锁
        self.lock_status = threading.Lock()
        # self.prcessed_url读写锁
        self.lock_url = threading.Lock()
        # sqlite 读写锁
        self.lock_sqlite = threading.Lock()

        # 放入目的url到线程池
        self.put_task((0, url))

    def stop(self):
        """
        具体的线程池终止实现
        :return:
        """
        # 进入终止状态
        with self.lock_status:
            self.task_status['stop'] = True

        # 用深度为-1的任务表示当前线程需终止
        for _ in range(len(self.threads)):
            # 该任务的优先级为1, 为最高，即立即处理该任务
            self.put_task((-1, ""), prior=1)

        # 等待线程终止
        for thread in self.threads:
            thread.join()

        # 清空任务队列残留任务(如果有)
        # clear_task()

        # 终止完成
        with self.lock_status:
            self.task_status['stop'] = False

    def thread_routing(self):
        """
        重写基类的线程执行函数。
        :return:
        """
        while True:

            # 任务队列的任务task是tuple类型
            # task[0] 该任务的优先级
            # task[1][0] 当前深度, 用-1表示线程需要终止，0表示初始页面
            # task[1][1] 要请求的URL
            task = self.task_queue.get()
            current_depth = task[1][0]
            target_url = task[1][1]

            # 收到终止任务，当前线程退出
            if current_depth == -1:
                self.task_queue.task_done()
                break

            # # 超过爬取深度，获取下一个任务（因为有可能还有目标深度内的任务）
            # if current_depth > self.depth:
            #     self.task_queue.task_done()
            #     continue

            # 已处理过的链接，不再处理
            with self.lock_url:
                if target_url in self.prcessed_url:
                    self.task_queue.task_done()
                    continue
                self.prcessed_url.append(target_url)

            # 更新任务状态
            thread_name = threading.current_thread().name
            with self.lock_status:
                self.task_status['crawl_st'][thread_name]['link_count'] += 1
                self.task_status['crawl_st'][thread_name]['cur_depth'] = current_depth

            # 处理链接
            try:
                self._process_link(target_url, current_depth)
            except Exception as e:
                print(e)
            self.task_queue.task_done()


    def _process_link(self, target_url, current_depth):
        """
        处理链接
        :param target_url: 链接的URL
        :param current_depth: 当前深度
        :return:
        """
        # 伪装成浏览器，以达到更好的爬取效果
        header = {
            "User-Agent": "Mozilla/5.0 (Windows NT 5.1; rv:18.0) Gecko/20100101 Firefox/18.0",
            "Connection":	"keep-alive"}
        # 转义含有中文的url
        target_url = quote(target_url, safe='/:?&;%=@#*')
        # 打开页面
        request = urllib.request.Request(url=target_url, headers=header)
        try:
            response = urllib.request.urlopen(request, timeout=_OPENURL_TIMEOUT)
        except Exception as e:
            _logger.error("Failed open: {} [{}]".format(target_url, e))
            return

        # 取得当前response的类型
        content_type = response.getheader('Content-Type')
        # 如果是页面则解析页面（到达指定深度时则无需再解析）
        if 'html' in content_type and current_depth < self.depth:
            try:
                self._parse_html(response, target_url, current_depth)
            except Exception as e:
                _logger.error("Failed parse: {} [{}]".format(target_url, e))
        # 如果是图片则保存图片
        elif 'image' in content_type:
            try:
                self._save_img(response, target_url)
            except Exception as e:
                _logger.error("Failed save image: {} [{}]".format(target_url, e))
            else:
                #更新任务状态
                thread_name = threading.current_thread().name
                with self.lock_status:
                    self.task_status['crawl_st'][thread_name]['img_count'] += 1

    def _parse_html(self, response, target_url, current_depth):
        """
        解析请求返回的页面
        :param response:
        :param target_url: 当前页面的URL
        :param current_depth: 当前深度
        :return:
        """
        # 尝试解码response, 传给lxml进行解析
        charset, html_text = self._decode_read(response)
        doc = lxml.html.document_fromstring(html_text)

        # 将该网页中的相对路径转换为绝对路径
        # 以便下一步的打开链接
        if not target_url.endswith('/'):
            target_url = target_url + '/'
        doc.make_links_absolute(target_url)

        # 解析图片链接
        img_nodes = doc.xpath("//img")
        for node in img_nodes:
            if 'src' not in node.attrib:
                continue

            # url格式有误的，忽略
            uf = urlparse(node.attrib['src'])
            if uf.scheme == '' or uf.netloc == '':
                _logger.warning("Uncorrect url [{}]".format(node.attrib['src']))
                continue

            # 检查是否允许爬取该url域
            if self.allowed_domain is None:
                # 加入任务队列，深度加1
                self.put_task((current_depth + 1, node.attrib['src']))
            else:
                if uf.netloc in self.allowed_domain:
                    # 加入任务队列，深度加1
                    self.put_task((current_depth + 1, node.attrib['src']))

        # 解析页面链接。（页面链接也有可能是图片）
        html_nodes = doc.xpath("//a")
        for node in html_nodes:
            if 'href' not in node.attrib:
                continue

            # url格式有误的，忽略
            uf = urlparse(node.attrib['href'])
            if uf.scheme == '' or uf.netloc == '':
                _logger.warning("Uncorrect url [{}]".format(node.attrib['href']))
                continue

             # 检查是否允许爬取该url域
            if self.allowed_domain is None:
                # 加入任务队列，深度加1，
                self.put_task((current_depth + 1, node.attrib['href']))
            else:
                if uf.netloc in self.allowed_domain:
                    # 加入任务队列，深度加1
                    self.put_task((current_depth + 1, node.attrib['href']))

    def _decode_read(self, response):
        """
        尝试对response进行解码
        :param response:
        :return charset: response的字符集
        :return html_text: response解码后的字符串
        """
        rdata = response.read()

        # 首选按网页返回字符集进行解码
        match = re.search("charset=(\w+)", response.getheader('Content-Type'))
        if match is not None:
            charset = match.group(1)
            try:
                html_text = rdata.decode(charset)
            except UnicodeError as e:
                charset = 'unknown'
        else:
            charset = 'unknown'

        # 未找到字符集或指定字符集不正确，尝试其他字符集
        if charset == 'unknown':
            try:
                html_text = rdata.decode("UTF-8")
            except UnicodeError as e:
                pass
            else:
                charset = 'UTF-8'

        if charset == 'unknown':
            try:
                html_text = rdata.decode("GBK")
            except UnicodeError as e:
                pass
            else:
                charset = "GBK"

        if charset == 'unknown':
            try:
                html_text = rdata.decode("ISO-8859-1")
            except UnicodeError as e:
                pass
            else:
                charset = 'ISO-8859-1'

        if charset == 'unknown':
            try:
                html_text = rdata.decode("gb18030")
            except UnicodeError as e:
                pass
            else:
                charset = 'gb18030'

        if charset == 'unknown':
            raise NotImplementedError("Cannot decode the page")
        return charset, html_text

    def _save_img(self, response, target_url):
        """
        从response读取图片，保存图片到数据库
        :param response:
        :param target_url: 该图片的url
        :return:
        """
        with self.lock_sqlite:
            _sqlite_wrapper.write(target_url, response.read())

    def status(self):
        """
        返回线程池当前的状态
        :return: self.task_status
        """
        with self.lock_status:
            # 获取任务队列当前的大小
            self.task_status["qsize"] = self.task_queue.qsize()
            # 获取每个线程的状态
            for thread in self.threads:
                self.task_status['crawl_st'][thread.name]['state'] = \
                    'alive' if thread.is_alive() else 'dead'
        return self.task_status

class SqliteWrapper:
    """
    Sqlite 包裹类，实现对 Sqlite 的打开、读写、关闭操作。
    """
    def __init__(self):
        # Sqlite 写入锁，以此实现线程安全的Sqlite写
        # （经测试，这种内部加锁的方式，不知何因，运行久了会出现阻塞的现象，
        # 改为外部调用者自己加锁后，基本没再出现阻塞。所以该锁就暂时没用到）
        self.lock = threading.Lock()

    def open(self, db_path):
        """
        打开sqlite
        :param db_path: sqlite数据库文件路径
        :return:
        """
        self.con = sqlite3.connect(db_path, check_same_thread=False)
        self.cur = self.con.cursor()
        # 建表resulte，列url保存图片的链接地址，列image保存图片内容
        self.cur.execute("create table if not exists resulte(url TEXT, image BLOB)")
        self.con.commit()

    def close(self):
        """
        关闭sqlite
        :return:
        """
        self.cur.close()
        self.con.close()

    def write(self, url, img):
        """
        写入图片到 Sqlite。
        :param url: 图片的URL
        :param img: 图片二进制数据
        :return:
        """
        # with self.lock:
        self.cur.execute("insert into resulte(url, image) values(?, ?)", (url, img))
        self.con.commit()

    def read(self, dir):
        """
        [测试用]
        将 Sqlite 中保存的图片提取到文件夹dir下，以便查看。
        :param dir: 目标文件夹
        :return: 提取的文件数量
        """
        self.cur.execute("select * from resulte")
        re_name = {}
        count = 0
        for row in self.cur:
            url = row[0]
            img = row[1]

            # 没有后缀名的图片名称，默认加上 .jpg 的后缀
            base_name = os.path.basename(urlparse(url)[2])
            if len(os.path.splitext(base_name)[1]) == 0:
                base_name += ".jpg"

            # 对于重名的图片名称，按照名称依次加1的方式重命名
            file_name = os.path.join(dir, base_name)
            if os.path.exists(file_name):
                if base_name not in re_name:
                    re_name[base_name] = 0
                re_name[base_name] += 1
                nms = os.path.splitext(base_name)
                file_name = os.path.join(dir, nms[0] + '-' + str(re_name[base_name])) + nms[1]

            with open(file_name, 'wb') as f:
                f.write(img)
            count += 1

        return count

class StatusThread(threading.Thread):
    """
    定时打印爬取状态的线程类
    """
    def __init__(self, downloader):
        super().__init__()
        # 指定爬虫实例
        self.downloader = downloader
        # 线程终止标志
        self.stop_flag = threading.Event()

    def run(self):
        """
        :return:
        """
        while True:
            # 收到终止命令
            if self.stop_flag.is_set():
                break

            # 每隔3秒打印一次统计信息
            self._format_output_status()
            time.sleep(3)

    def _format_output_status(self):
        """
        按一定格式打印出各线程进度以及总计信息
        :return:
        """
        # 线程安全起见，使用深拷贝
        task_status = copy.deepcopy(self.downloader.status())
        # 打印任务队列数
        print("Task queue size: %d" % task_status["qsize"])
        # 设置各线程的显示格式
        line = "{0:<10}{1:>15}{2:>15}{3:>15}{4:>10}"
        print(line.format('Thread', 'Link Count', 'Image Count', 'Current Depth', 'State'))
        print(line.format('------', '----------', '-----------', '-------------', '-----'))
        # 链接和图片总数
        t_suc_link, t_suc_pic = 0, 0
        # 按线程名的次序依次打印
        for i in range(len(task_status['crawl_st'])):
            td_name = 'Thread-' + str(i + 1)
            print(line.format(td_name,
                              task_status['crawl_st'][td_name]['link_count'],
                              task_status['crawl_st'][td_name]['img_count'],
                              task_status['crawl_st'][td_name]['cur_depth'],
                              task_status['crawl_st'][td_name]['state']))
            # 计算总数
            t_suc_link += task_status['crawl_st'][td_name]['link_count']
            t_suc_pic += task_status['crawl_st'][td_name]['img_count']
        # 打印总数
        print(line.format('Total', t_suc_link, t_suc_pic, '', ''))
        # 终止提示
        if task_status['stop']:
            print("[Stoping thread pool, please wait...]")
        else:
            print("[Press Ctrl-C to exit]")
        print("")

    def stop(self):
        """
        终止线程
        :return:
       """
        self.stop_flag.set()
        self.join()

def _init_logger(level, log_file):
    """
    初始化日志
    :return:
    """
    global _logger
    _logger = logging.getLogger('Crawler')
    formatter = logging.Formatter('%(name)s %(asctime)s %(levelname)s %(message)s')
    handler = logging.handlers.RotatingFileHandler(log_file, maxBytes=1024*1024*100, backupCount=10)
    handler.setFormatter(formatter)
    _logger.addHandler(handler)

    '''
    只有小于指定级别的日志才会被记录
    所以指定级别越高，记录越详细
    参考python3文档，级别与数字的对应关系为
    Level Numeric    value
    -------------    -----
    CRITICAL         50
    ERROR            40
    WARNING          30
    INFO             20
    DEBUG            10
    NOTSET           0
    '''
    _logger.setLevel((6-level)*10)

def _parse_args():
    """
    参数解析
    :return:
    """
    import argparse
    parser = argparse.ArgumentParser(
        description="A python script crawling the web, starting from a specified URL, downloading any"
                    " image and store in sqlite database.")
    parser.add_argument('-u', '--url', required=True, help="the starting URL.")
    parser.add_argument('-d', '--deep', type=int, required=True, help="crawling deep.")
    parser.add_argument('--thread', type=int, default=10, help="crawling thread pool thread number, default 10.")
    parser.add_argument('--dbfile', default="crawler.db", help="sqlite db path, default crawler.db.")
    parser.add_argument('-o', '--timeout', type=int, default=10, help="http request timeout, default 10.")
    parser.add_argument('-f', '--logfile', default="crawler.log", help="log file path, default crawler.log.")
    parser.add_argument('-l', '--loglevel', choices=[1,2,3,4,5], default=5, help="log level, default 5.")
    parser.add_argument('-ne', '--no-cross-domain-except', nargs="*", help=\
        "disable cross domain crawling, except the specified domains. eg:\n"
        "./crawler.py, allow crawling any domain.\n"
        "./crawler.py -ne, only allow crawling the --url specified domain.\n"
        "./crawler.py -ne example1.com example2.com, allow those two domain and the --url specified domain.")
    return parser.parse_args()

if __name__ == '__main__':
    import lxml.etree
    import lxml.html

    # 设置请求超时值
    args = _parse_args()
    _OPENURL_TIMEOUT = args.timeout

    # 初始化日志
    _init_logger(args.loglevel, args.logfile)

    # 打开sqlite数据库
    _sqlite_wrapper = SqliteWrapper()
    try:
        _sqlite_wrapper.open(args.dbfile)
    except Exception as e:
        print("Cannot open sqlite file {} [{}]".format(args.dbfile, e))
        _sqlite_wrapper.close()
        sys.exit(-1)

    # 构建爬取线程池和状态打印线程
    downloader = DownloadPool(args.thread, args.url, args.deep, args.no_cross_domain_except)
    status_thread = StatusThread(downloader)

    # 启动各线程，开始计时（为后面计算爬取总耗时）
    start = time.time()
    status_thread.start()
    downloader.start()

    # 进入线程池等待
    try:
        downloader.wait()
    except KeyboardInterrupt:
        print()
        print("[Stoping thread pool...]\n")
        downloader.stop()
    status_thread.stop()

    # 打印爬取总耗时，关闭sqlite
    end = time.time()
    print("Total consumed time: %d seconds." % (end - start))
    _sqlite_wrapper.close()

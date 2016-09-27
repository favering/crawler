#! /usr/bin/env python3
# -*- coding: gb18030 -*-

import queue
import urllib.request
import time
import threading
import re
import lxml.etree
import lxml.html
from urllib.parse import urlparse, quote
import os
import logging
import logging.handlers
import copy
import sqlite3

# ��־����
_logger = None
# sqlite��д����
_sqlite_wrapper = None
# ҳ������timeout
_OPENURL_TIMEOUT = None

class ThreadPool:
    """
    �̳߳ػ���
    """
    def __init__(self, thread_num):
        self.threads = \
            [threading.Thread(target=self.thread_routing) for _ in range(thread_num)]

        # task_queueΪPriorityQueue��
        # ��Ҫ��Ϊ���ֳ����������ֹ����ͬ�����ȼ�
        self.task_queue = queue.PriorityQueue()

    def thread_routing(self):
        """
        �����̵߳�ִ�к���
        :return:
        """
        pass

    def put_task(self, raw, prior=2):
        """
        ������񵽶���
        :param raw: ��������
        :param prior: �������ȼ���
                      ������������ȼ���Ϊ 2��Ĭ�ϡ�
                      �߳���ֹ��������ȼ��ϸߣ���Ϊ 1�� �����յ���ֹ��������˳�
        :return:
        """
        self.task_queue.put((prior, raw))

    def start(self):
        """
        ���������߳�
        :return:
        """
        for thread in self.threads:
            thread.start()

    def wait(self):
        """
        �ȴ��������ȫ�����
        :return:
        """
        self.task_queue.join()
        self.stop()

    def stop(self):
        """
        ��ֹ�̳߳�
        :return:
        """
        pass

class DownloadPool(ThreadPool):
    """
    ��ҳ��ȡ�̳߳�
    """
    def __init__(self, thread_num, depth):
        super().__init__(thread_num)

        # Ŀ����ȡ���
        self.depth = depth

        # �������url����
        self.prcessed_url = []

        # �̳߳ص�ǰ״̬�ֵ�
        # qsize ��ǰ�����е�������
        # stop �̳߳��Ƿ������ֹ״̬(True:������ֹ False:δ������ֹ)
        self.task_status = {}
        self.task_status["qsize"] = 0
        self.task_status['stop'] = False

        # crawl_st ���߳���ȡ״̬�ֵ䣬��ӵ�self.task_status
        # key           �߳���
        # link_count    ���̴߳����������
        # img_count     ���߳����ص�ͼƬ��
        # cur_depth     ���̵߳�ǰ���
        # state         ���̵߳Ĵ��״̬
        crawl_st = {}
        for thread in self.threads:
            crawl_st[thread.name] = \
                {'link_count': 0, 'img_count': 0, 'cur_depth': 0, 'state': 'dead'}
        self.task_status['crawl_st'] = crawl_st

        # self.task_status��д��
        self.lock_status = threading.Lock()
        # self.prcessed_url��д��
        self.lock_url = threading.Lock()
        # sqlite ��д��
        self.lock_sqlite = threading.Lock()

    def stop(self):
        """
        ������̳߳���ֹʵ��
        :return:
        """
        # ������ֹ״̬
        with self.lock_status:
            self.task_status['stop'] = True

        # �����Ϊ-1�������ʾ��ǰ�߳�����ֹ
        for _ in range(len(self.threads)):
            # ����������ȼ�Ϊ1, Ϊ��ߣ����������������
            self.put_task((-1, ""), prior=1)

        # �ȴ��߳���ֹ
        for thread in self.threads:
            thread.join()

        # ���������в�������(�����)
        # clear_task()

        # ��ֹ���
        with self.lock_status:
            self.task_status['stop'] = False

    def thread_routing(self):
        """
        ��д������߳�ִ�к�����
        :return:
        """
        while True:

            # ������е�����task��tuple����
            # task[0] ����������ȼ�
            # task[1][0] ��ǰ���, ��-1��ʾ�߳���Ҫ��ֹ��0��ʾ��ʼҳ��
            # task[1][1] Ҫ�����URL
            task = self.task_queue.get()
            current_depth = task[1][0]
            target_url = task[1][1]

            # �յ���ֹ���񣬵�ǰ�߳��˳�
            if current_depth == -1:
                self.task_queue.task_done()
                break

            # # ������ȡ��ȣ���ȡ��һ��������Ϊ�п��ܻ���Ŀ������ڵ�����
            # if current_depth > self.depth:
            #     self.task_queue.task_done()
            #     continue

            # �Ѵ���������ӣ����ٴ���
            with self.lock_url:
                if target_url in self.prcessed_url:
                    self.task_queue.task_done()
                    continue
                self.prcessed_url.append(target_url)

            # ��������״̬
            thread_name = threading.current_thread().name
            with self.lock_status:
                self.task_status['crawl_st'][thread_name]['link_count'] += 1
                self.task_status['crawl_st'][thread_name]['cur_depth'] = current_depth

            # ��������
            try:
                self._process_link(target_url, current_depth)
            except Exception as e:
                print(e)
            self.task_queue.task_done()


    def _process_link(self, target_url, current_depth):
        """
        ��������
        :param target_url: ���ӵ�URL
        :param current_depth: ��ǰ���
        :return:
        """
        # αװ����������Դﵽ���õ���ȡЧ��
        header = {
            "User-Agent": "Mozilla/5.0 (Windows NT 5.1; rv:18.0) Gecko/20100101 Firefox/18.0",
            "Connection":	"keep-alive"}
        # ת�庬�����ĵ�url
        target_url = quote(target_url, safe='/:?&;%=@#*')
        # ��ҳ��
        request = urllib.request.Request(url=target_url, headers=header)
        try:
            response = urllib.request.urlopen(request, timeout=_OPENURL_TIMEOUT)
        except Exception as e:
            _logger.error("Failed open: {} [{}]".format(target_url, e))
            return

        # ȡ�õ�ǰresponse������
        content_type = response.getheader('Content-Type')
        # �����ҳ�������ҳ�棨����ָ�����ʱ�������ٽ�����
        if 'html' in content_type and current_depth < self.depth:
            try:
                self._parse_html(response, target_url, current_depth)
            except Exception as e:
                _logger.error("Failed parse: {} [{}]".format(target_url, e))
        # �����ͼƬ�򱣴�ͼƬ
        elif 'image' in content_type:
            try:
                self._save_img(response, target_url)
            except Exception as e:
                _logger.error("Failed save image: {} [{}]".format(target_url, e))
            else:
                #��������״̬
                thread_name = threading.current_thread().name
                with self.lock_status:
                    self.task_status['crawl_st'][thread_name]['img_count'] += 1

    def _parse_html(self, response, target_url, current_depth):
        """
        �������󷵻ص�ҳ��
        :param response:
        :param target_url: ��ǰҳ���URL
        :param current_depth: ��ǰ���
        :return:
        """
        # ���Խ���response, ����lxml���н���
        charset, html_text = self._decode_read(response)
        doc = lxml.html.document_fromstring(html_text)

        # ������ҳ�е����·��ת��Ϊ����·��
        # �Ա���һ���Ĵ�����
        if not target_url.endswith('/'):
            target_url = target_url + '/'
        doc.make_links_absolute(target_url)

        # ����ͼƬ���ӣ�����������У���ȼ�1
        img_nodes = doc.xpath("//img")
        for node in img_nodes:
            if 'src' in node.attrib:
                self.put_task((current_depth + 1, node.attrib['src']))

        # ����ҳ�����ӣ�����������У���ȼ�1
        html_nodes = doc.xpath("//a")
        for node in html_nodes:
            if 'href' in node.attrib:
                self.put_task((current_depth + 1, node.attrib['href']))

    def _decode_read(self, response):
        """
        ���Զ�response���н���
        :param response:
        :return charset: response���ַ���
        :return html_text: response�������ַ���
        """
        rdata = response.read()

        # ��ѡ����ҳ�����ַ������н���
        match = re.search("charset=(\w+)", response.getheader('Content-Type'))
        if match is not None:
            charset = match.group(1)
            html_text = rdata.decode(charset)
        # ���û�з����ַ��������������ַ���
        else:
            charset = 'unknown'
            if charset == 'unknown':
                try:
                    html_text = rdata.decode("UTF-8")
                except UnicodeError as e:
                    pass
                else:
                    charset = 'UTF-8'

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
        ��response��ȡͼƬ������ͼƬ�����ݿ�
        :param response:
        :param target_url: ��ͼƬ��url
        :return:
        """
        with self.lock_sqlite:
            _sqlite_wrapper.write(target_url, response.read())

    def status(self):
        """
        �����̳߳ص�ǰ��״̬
        :return: self.task_status
        """
        with self.lock_status:
            # ��ȡ������е�ǰ�Ĵ�С
            self.task_status["qsize"] = self.task_queue.qsize()
            # ��ȡÿ���̵߳�״̬
            for thread in self.threads:
                self.task_status['crawl_st'][thread.name]['state'] = \
                    'alive' if thread.is_alive() else 'dead'
        return self.task_status

class SqliteWrapper:
    """
    Sqlite �����࣬ʵ�ֶ� Sqlite �Ĵ򿪡���д���رղ�����
    """
    def __init__(self):
        # Sqlite д�������Դ�ʵ���̰߳�ȫ��Sqliteд
        # �������ԣ������ڲ������ķ�ʽ����֪�������о��˻��������������
        # ��Ϊ�ⲿ�������Լ������󣬻���û�ٳ������������Ը�������ʱû�õ���
        self.lock = threading.Lock()

    def open(self, db_path):
        """
        ��sqlite
        :param db_path: sqlite���ݿ��ļ�·��
        :return:
        """
        self.con = sqlite3.connect(db_path, check_same_thread=False)
        self.cur = self.con.cursor()
        # ����resulte����url����ͼƬ�����ӵ�ַ����image����ͼƬ����
        self.cur.execute("create table if not exists resulte(url TEXT, image BLOB)")
        self.con.commit()

    def close(self):
        """
        �ر�sqlite
        :return:
        """
        self.cur.close()
        self.con.close()

    def write(self, url, img):
        """
        д��ͼƬ�� Sqlite��
        :param url: ͼƬ��URL
        :param img: ͼƬ����������
        :return:
        """
        # with self.lock:
        self.cur.execute("insert into resulte(url, image) values(?, ?)", (url, img))
        self.con.commit()

    def read(self, dir):
        """
        [������]
        �� Sqlite �б����ͼƬ��ȡ���ļ���dir�£��Ա�鿴��
        :param dir: Ŀ���ļ���
        :return: ��ȡ���ļ�����
        """
        self.cur.execute("select * from resulte")
        re_name = {}
        count = 0
        for row in self.cur:
            url = row[0]
            img = row[1]

            # û�к�׺����ͼƬ���ƣ�Ĭ�ϼ��� .jpg �ĺ�׺
            base_name = os.path.basename(urlparse(url)[2])
            if len(os.path.splitext(base_name)[1]) == 0:
                base_name += ".jpg"

            # ����������ͼƬ���ƣ������������μ�1�ķ�ʽ������
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
    ��ʱ��ӡ��ȡ״̬���߳���
    """
    def __init__(self, downloader):
        super().__init__()
        # ָ������ʵ��
        self.downloader = downloader
        # �߳���ֹ��־
        self.stop_flag = threading.Event()

    def run(self):
        """
        :return:
        """
        while True:
            # �յ���ֹ����
            if self.stop_flag.is_set():
                break

            # ÿ��3���ӡһ��ͳ����Ϣ
            self._format_output_status()
            time.sleep(3)

    def _format_output_status(self):
        """
        ��һ����ʽ��ӡ�����߳̽����Լ��ܼ���Ϣ
        :return:
        """
        # �̰߳�ȫ�����ʹ�����
        task_status = copy.deepcopy(self.downloader.status())
        # ��ӡ���������
        print("Task queue size: %d" % task_status["qsize"])
        # ���ø��̵߳���ʾ��ʽ
        line = "{0:<10}{1:>15}{2:>15}{3:>15}{4:>10}"
        print(line.format('Thread', 'Link Count', 'Image Count', 'Current Depth', 'State'))
        print(line.format('------', '----------', '-----------', '-------------', '-----'))
        # ���Ӻ�ͼƬ����
        t_suc_link, t_suc_pic = 0, 0
        # ���߳����Ĵ������δ�ӡ
        for i in range(len(task_status['crawl_st'])):
            td_name = 'Thread-' + str(i + 1)
            print(line.format(td_name,
                              task_status['crawl_st'][td_name]['link_count'],
                              task_status['crawl_st'][td_name]['img_count'],
                              task_status['crawl_st'][td_name]['cur_depth'],
                              task_status['crawl_st'][td_name]['state']))
            # ��������
            t_suc_link += task_status['crawl_st'][td_name]['link_count']
            t_suc_pic += task_status['crawl_st'][td_name]['img_count']
        # ��ӡ����
        print(line.format('Total', t_suc_link, t_suc_pic, '', ''))
        # ��ֹ��ʾ
        if task_status['stop']:
            print("[Stoping thread pool, please wait...]")
        else:
            print("[Press Ctrl-C to exit]")
        print("")

    def stop(self):
        """
        ��ֹ�߳�
        :return:
       """
        self.stop_flag.set()
        self.join()

def _init_logger(level, log_file):
    """
    ��ʼ����־
    :return:
    """
    global _logger
    _logger = logging.getLogger('Crawler')
    formatter = logging.Formatter('%(name)s %(asctime)s %(levelname)s %(message)s')
    handler = logging.handlers.RotatingFileHandler(log_file, maxBytes=1024*1024*100, backupCount=10)
    handler.setFormatter(formatter)
    _logger.addHandler(handler)

    '''
    ֻ��С��ָ���������־�Żᱻ��¼
    ����ָ������Խ�ߣ���¼Խ��ϸ
    �ο�python3�ĵ������������ֵĶ�Ӧ��ϵΪ
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
    ��������
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
    return parser.parse_args()

if __name__ == '__main__':
    import sys

    # ��������ʱֵ
    args = _parse_args()
    _OPENURL_TIMEOUT = args.timeout

    # ��ʼ����־
    _init_logger(args.loglevel, args.logfile)

    # ��sqlite���ݿ�
    _sqlite_wrapper = SqliteWrapper()
    try:
        _sqlite_wrapper.open(args.dbfile)
    except Exception as e:
        print("Cannot open sqlite file {} [{}]".format(args.dbfile, e))
        _sqlite_wrapper.close()
        sys.exit(-1)

    # ������ȡ�̳߳غ�״̬��ӡ�߳�
    downloader = DownloadPool(thread_num=args.thread, depth=args.deep)
    status_thread = StatusThread(downloader)

    # �������̣߳���ʼ��ʱ��Ϊ���������ȡ�ܺ�ʱ��
    start = time.time()
    status_thread.start()
    downloader.start()

    # �����ʼurl���̳߳أ�����ʹ��������url��ʽ��
    if urlparse(args.url).scheme == '':
        args.url = "http://" + args.url
    downloader.put_task((0, args.url))
    # �����̳߳صȴ�
    try:
        downloader.wait()
    except KeyboardInterrupt:
        print()
        print("[Stoping thread pool...]\n")
        downloader.stop()
    status_thread.stop()

    # ��ӡ��ȡ�ܺ�ʱ���ر�sqlite
    end = time.time()
    print("Total consumed time: %d seconds." % (end - start))
    _sqlite_wrapper.close()

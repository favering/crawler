# crawler.py
一个网络爬虫程序。从指定的URL开始，按照指定的深度进行爬取，将所有爬到的图片下载保存在sqlite数据库中。

为不漏掉图片，该爬虫会对所有的页面链接进行请求，凡是遇到返回content-type为image的，都会进行保存。

usage:
输入"crawler.py -h"查看使用方法

# sqlite_reader.py
将crawler.py保存在sqlite数据库中的图片提取到指定文件夹下，以便查看。

usage:
输入"sqlite_reader.py -h"查看使用方法

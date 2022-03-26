"""
2020-5-12

爬取百度百科每个实体页面下的所有a标签(表示link标签)
"""
import os
import re
import json
import time
import requests
import random
import pandas as pd
from bs4 import BeautifulSoup
import urllib.parse
from collections import defaultdict
from multiprocessing import Process, Queue, Manager, Lock
import pymongo
lock = Lock()
import asyncio, aiohttp


def construct_url(keyword):
    baseurl = 'https://baike.baidu.com/item/'
    url = baseurl + str(keyword)
    return url

async def main_crawler(url, yixiang, request_headers, db, olds):
    """对知识库中的每一个subject，访问其百度百科页面，然后获取页面下所有的超链接"""
    link_list = []

    times = 0
    while True:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers=request_headers) as response:
                    req_text = await response.text()
                    break
        except:
            print("#####Please wait 3 seconds#####")
            time.sleep(1)
            times += 1
            if times >= 3:
                return None

    soup = BeautifulSoup(req_text, 'lxml')
    if soup.find('div', attrs={'class': 'lemmaWgt-subLemmaListTitle'}):
        # 说明页面进入到多义词列表的页面，需要从多义词列表中找到匹配待检索实体的义项描述的链接
        li_label = soup.find_all('li', attrs={'class': 'list-dot list-dot-paddingleft'})
        for li in li_label:
            para_label = li.find('div', attrs={'class': 'para'})
            text = para_label.get_text()
            res = text.split('：')[-1]
            if res == yixiang:
                a_label = li.find('a')
                redirect_url = "https://baike.baidu.com" + a_label['href']
                # 重新获取到该subject对应页面的链接地址，访问获取到页面下的所有超链接
                entity_link_list = await get_page_link(redirect_url, request_headers)
                link_list = await iterate_all_page_links(entity_link_list, request_headers, db, olds)
                return link_list
    elif soup.find('ul', attrs={'class': 'polysemantList-wrapper cmn-clearfix'}):
        # 说明进入了对应subject的百科页面，如果是多义词找到对应义项描述的链接网页
        ul_label = soup.find('ul', attrs={'class': 'polysemantList-wrapper cmn-clearfix'})
        li_label = ul_label.find_all('li', attrs={'class': 'item'})
        for li in li_label:
            text = li.get_text().strip('▪')
            if text == yixiang:
                if not li.find('a'):
                    # 未发现a标签，则说明是当前页面下，则直接使用原始的Url链接
                    entity_link_list = await get_page_link(url, request_headers)
                    link_list = await iterate_all_page_links(entity_link_list, request_headers, db, olds)
                    return link_list
                else:
                    # 否则获取新的重定向链接
                    a_label = li.find('a')
                    redirect_url = "https://baike.baidu.com" + a_label['href']
                    entity_link_list = await get_page_link(redirect_url, request_headers)
                    link_list = await iterate_all_page_links(entity_link_list, request_headers, db, olds)
                    return link_list
            else:
                if li.find('a'):
                    a_label = li.find('a')
                    redirect_url = "https://baike.baidu.com" + a_label['href']
                    entity_link_list = await get_page_link(redirect_url, request_headers)
                    link_list = await iterate_all_page_links(entity_link_list, request_headers, db, olds)
                    return link_list

    elif soup.find('dd', attrs={'class':'lemmaWgt-lemmaTitle-title'}):
        # 如果subject对应的页面是单义词，则直接获取页面下的所有超链接
        entity_link_list = await get_page_link(url, request_headers)
        link_list = await iterate_all_page_links(entity_link_list, request_headers, db, olds)
        return link_list
    else:
        # 可能是未知页面，返回None
        # 存在是多义词，在其义项描述不在百度百科多义词列表里。
        return None

    if len(link_list) == 0:
        # 说明百度百科页面中没有相应的义项描述与之对应，返回None
        return None



async def get_page_link(url, request_headers):
    # entity_link_dict = defaultdict(list)
    entity_link_list = []
    times = 0
    while True:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers=request_headers) as response:
                    req_text = await response.text()
                    break
        except:
            print("#####Please wait 3 seconds#####")
            time.sleep(1)
            times += 1
            if times >= 3:
                return None
    # req_text = response.text
    try:
        soup = BeautifulSoup(req_text, 'lxml')
        main_content = soup.find('div', attrs={'class':'main-content'})
        a_label = main_content.find_all('a', attrs={'target':'_blank'})
    except:
        return []
    for a_tag in a_label:
        try:
            href = a_tag['href']
            href_val = validate_href(href)
            if not href_val:
                continue
            link_href = href
            entity_link_list.append(link_href)
        except:
            continue

    return entity_link_list

async def iterate_all_page_links(link_list, request_headers, db_all, olds):
    """对于某一个页面下的所有超链接，获取每个超链接的href、页面title和义项描述"""
    title_href = "https://baike.baidu.com"
    baike_id_list = []
    link_data = []
    for link in link_list:
        item_dict = dict()
        # baike_id = link.split('/')[-1]  # 获取该实体在百度百科知识库中的id
        # if link in baike_id_list:
            # 避免访问重复的超链接
            # continue
        # baike_id_list.append(link)
        quote = link.split('/')[2]    # 获取链接的标题
        link_title = urllib.parse.unquote(quote)
        href = title_href + link
        # if tr.search(href):
        #     print(f"duplicate: {href+link_title}")
        #     continue
        if href in record:
            print(f"duplicate: {href+link_title}")
            continue

        flag = True
        times = 0
        while True:
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(href, headers=request_headers) as response:
                        req_text = await response.text()
                        break
            except:
                print("#####Please wait 3 seconds#####")
                flag = False
                time.sleep(1)
                times += 1
                if times >= 3:
                    return None

        # while True:
        #     try:
        #         req_text = requests.get(href, headers=request_headers).text
        #         break
        #     except:
        #         flag = False
        #         print("#####Please wait 3 seconds#####")
        #         time.sleep(3)
        if not flag:
            # 如果目标超链接页面无效的，则直接跳过该页面
            continue
        # req_text = response.text
        soup = BeautifulSoup(req_text, 'lxml')
        # 得到每个链接对应的义项描述
        link_label = await get_link_label(soup)
        if link_label is None:
            continue
        item_dict['Link'] = href
        item_dict['Title'] = link_title
        item_dict['Label'] = link_label


        # lock.acquire()
        _id = f"{link_title}-{link_label}" if link_label != "monoseme" else link_title
        if _id in olds or href in record: continue
        link_data.append(item_dict)

        link_label = link_label if link_label != "monoseme" else "单义词"


        try:
            db_all.insert_one(
                {
                    '_id': _id,
                    # 'text': ''.join(response.xpath('//div[@class="lemma-summary"]').xpath('//div[@class="para"]//text()').getall())
                    'link': href,
                    'title': link_title,
                    'label': link_label,
                    'text': req_text
                })
            print(f"insert: {href+link_title}")
            olds.add(f"{link_title}-{link_label}" if link_label != "单义词" else link_title) 
        except pymongo.errors.DuplicateKeyError:
            print(f"duplicate insert: {href+link_title}")
            # tr.insert(href)
            record.add(href)
            olds.add(f"{link_title}-{link_label}")
            pass
        # lock.release()

    return link_data



async def get_link_label(soup):
    """获取到对应百度百科页面实体的义项描述，来作为出现多义词时的唯一标识"""
    if soup.find('div', attrs={'class':'lemma-summary'}) is None:
        # 说明出现错误页面，比如页面不存在等情况
        return None
    dd_label = soup.find('dd', attrs={'class':'lemmaWgt-lemmaTitle-title'})
    h_label = dd_label.find_all('h1')
    if len(h_label) == 2:
        # 说明title旁边存在括号，以此作为页面的义项描述
        h2_tag = h_label[1].get_text()
        h2_tag_strip = h2_tag.strip('（）')
        return h2_tag_strip
    elif soup.find('ul', attrs={'class': 'polysemantList-wrapper cmn-clearfix'}):
        # 说明该页面仍然是一个多义词页面，但是页面布局的形式不同，采取不同的获取方法
        ul_label = soup.find('ul', attrs={'class': 'polysemantList-wrapper cmn-clearfix'})
        li_label = ul_label.find_all('li', attrs={'class': 'item'})
        for li in li_label:
            if li.find('span'):
                # <span>标签表示了该文本内容为当前页面，否则会是<a>标签
                text = li.get_text().strip('▪')
                return text
    else:
        # 对于非多义词界面，义项描述返回monoseme
        yixiang = 'monoseme'    # 以该标签标识该词在百度百科中为单义词
        return yixiang



def validate_href(href):
    """对标签内容进行检查，对于href内没有item项的href进行过滤"""
    item_compile = re.compile(r'^/item/.+') # 尾匹配，排除干扰的超链接
    if item_compile.search(href):
        return True
    else:
        return False

def page_type(soup):
    # 对于多义词列表的页面进行解析
    if soup.find('div', attrs={'class': 'lemmaWgt-subLemmaListTitle'}):
        return 1
    # 对于页面最上面有多义词title的页面进行解析
    elif soup.find('ul', attrs={'class':'polysemantList-wrapper cmn-clearfix'}):
        return 2
    else:
        # 对于不是多义词，只有每一个词的页面
        return 3


class Input:
    def __init__(self, link, title, label):
        self.link = link
        self.title = title
        self.label = label

    def __str__(self):
        return self.link

class Node:  # 辅助树节点
    def __init__(self, val=None, isEnd=False):
        self.val = val  # 当前值
        self.next = {}  # 子集合, dict形式, 比如apple就是 {'a': Node('p')}, {'p': Node('p')}
        self.isEnd = isEnd  # 是否是结尾


class Trie:
    def __init__(self):
        self.node = Node()

    def insert(self, word):
        tmp = self.node  # 初始节点None, 其子集合为单词开头
        for i in word:
            if i not in tmp.next:
                tmp.next[i] = Node(i)  # 将当前字母插入子集合里
            tmp = tmp.next[i]
        tmp.isEnd = True

    def search(self, word):
        tmp = self.node
        for i in word:
            if i not in tmp.next:
                return False
            tmp = tmp.next[i]
        if tmp.isEnd:  # 与上两行属于一套的
            return True
        return False

    def startsWith(self, prefix):
        tmp = self.node
        for i in prefix:
            if i not in tmp.next:
                return False
            tmp = tmp.next[i]
        return True


tr = Trie()


def load():
    zh_seeds = ['猫', '薄一波', '路飞', '铁树', '梅花', '阿卡迪亚的牧人', '隆中对', '台球', '足球', '北京大学', '贝勒大学',
                '狗', '鸟', '中国', '美国', '日本', '唐朝', '宋朝', '南极', '北极', '太平洋', '泰山', '世界大战', '北京', '四川',
                '血液病', '癌症', '甲苯', '染色体', '大数定律', '计算机', '冰川', '第二次世界大战', '太岁', '腾讯', '黑洞', '台风',
                '地震', '雪崩', '泥石流']
    with open("seeds.txt", "r", encoding="utf-8") as f:
        for line in f:
            zh_seeds.append(line.strip())
    zh_seeds = set(zh_seeds)
    output = [Input(
        link="https://baike.baidu.com/item/" + title,
        title=title,
        label=""
    ) for title in zh_seeds]
    # output = set(output)
    print(f"output lens: {len(output)}")
    return output




class CrawlerProcess(Process):
    def __init__(self, request_headers, olds, url_list, record, lock):
        """
        :param id_list: 包含实体id的实体池
        :param q: 每个进程保存爬取结果的队列
        :param lock: 进程锁
        :param id2subject: 实体id与实体名间的映射dict
        :param describe_dict: 包含义项描述（即实体消歧信息）的dict
        :param request_headers: requests访问请求头
        """
        Process.__init__(self)
        self.request_headers = request_headers
        self.lock = lock
        # self.db = db
        self.olds = olds
        self.url_list = url_list
        self.record = record

    def run(self):
        # # 每一个进程不断从实体池中取实体，直到实体池为空
        # db = pymongo.MongoClient("mongodb://zj184x.corp.youdao.com:30000/")["chat_baike"]
        # # db_all = db['triple']
        # db_all = db['test1']
        # 每一个进程不断从实体池中取实体，直到实体池为空
        db = pymongo.MongoClient("mongodb://zj184x.corp.youdao.com:30000/")["chat_baike"]
        # db_all = db['triple']
        db_all = db['test1']
        # olds = set([item['_id'] for item in db_all.find({}, {'_id': 1})])
        # print(len(olds))

        # url_list = [
        #     {
        #         "Link": "https://baike.baidu.com/item/%E4%B8%AD%E5%8D%8E%E4%BA%BA%E6%B0%91%E5%85%B1%E5%92%8C%E5%9B%BD",
        #         "Title": "中国",
        #         "Label": "中华人民共和国"
        #     }
        # ]

        # url_list = [Input(link="https://baike.baidu.com/item/%E4%B8%AD%E5%8D%8E%E4%BA%BA%E6%B0%91%E5%85%B1%E5%92%8C%E5%9B%BD",
        #                   title="中国",
        #                   label="中华人民共和国")]
        # url_list = self.gen_url_list

        # tr.insert(url_list[0])
        # mark_list = {"中国-中华人民共和国"}

        while len(self.url_list) != 0:
            # 加锁
            #self.lock.acquire()
            #if len(self.url_list) == 0:
                # 额外的一个退出判断，防止出现只有最后一个实体，但有多个进程进入了while循环的情况
            #    self.lock.release()
            #    break
            # 从实体池中随机选取一个实体
            # url_info = url_list[-1]
            # url_info = random.choice(url_list)
            url_info = self.url_list.pop()
            # 选完后删除
            # url_list.remove(url_info)
            # url_list.pop()
            # 解锁
            #self.lock.release()
            # if tr.search(url_info.link): continue
            if url_info.link in self.record: continue

            # 由实体id转换为对应的实体名
            subject = url_info.title
            # 这里的义项描述，则表示额外的消歧信息，来帮助获取到正确的对应页面
            yixiang = url_info.label
            # 根据实体名，构造百度百科访问地址
            if f"{subject}-{yixiang}" in olds: continue
            url = construct_url(keyword=subject)
            # 对于每个subject，获取符合其义项描述的对应页面下的所有超链接
            link_data = asyncio.get_event_loop().run_until_complete(
                main_crawler(url, yixiang, self.request_headers, db_all, self.olds))
            # entity_link_dict = dict()
            # if link_data is None or len(link_data) == 0:
            #     # 可能有页面没有超链接的情况
            #     entity_link_dict[choice_id] = "Null"
            # else:
            #     entity_link_dict[choice_id] = link_data

            if link_data:
                for info in link_data:
                    # if tr.search(info["Link"]): continue
                    if info["Link"] in self.record: continue
                    # url_list.append(Input(
                    #     link=info["Link"],
                    #     title=info["Title"],
                    #     label=info["Label"]
                    # ))
                    self.url_list.add(Input(
                        link=info["Link"],
                        title=info["Title"],
                        label=info["Label"]
                    ))
                    self.record.add(info["Link"])

                    # tr.insert(info["Link"])


if __name__ == '__main__':
    request_headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed- xchange;v=b3;q=0.9',
        'Accept - Encoding': 'gzip, deflate, br',
        'Accept - Language': 'zh-CN,zh;q=0.9',
        'Cache - Control': 'max-age=0',
        'Connection': 'keep-alive',
        'Cookie': 'BAIDUID=003D94039A5FB16CE650EBCF5E72A45E:FG=1; BIDUPSID=003D94039A5FB16CE650EBCF5E72A45E; PSTM=1561885291; BDUSS=WVXUDRZSHdKeVJhaERyOXh2TTNoT3lPN3p4VE04SXhKSlVUWTd4S2JMMmtLMEZkSVFBQUFBJCQAAAAAAAAAAAEAAABh2fss1OfJz7XEtrm9rNPNzPUAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAKSeGV2knhldQ; H_PS_PSSID=; delPer=0; BD_CK_SAM=1; PSINO=2; BDRCVFR[BCzcNGRrF63]=mk3SLVN4HKm; BD_HOME=1; BD_UPN=12314753; BDORZ=FFFB88E999055A3F8A630C64834BD6D0; COOKIE_SESSION=2691823_0_9_0_66_16_0_2_9_5_115_2_3625418_0_2_0_1588747660_0_1588747658%7 C9%23191_140_1583480922%7C9; H_PS_645EC=b082T2nk%2FHreRxzRLh%2F4Lvy%2FrJ0eUckomxoWqhlovZkh4zkgCdpy%2FXI9AIKSPp5b9I1IZ3c; BDSVRTM=193',
        'Host': 'baike.baidu.com',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'same-origin',
        'Sec-Fetch-User': '?1',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0(Windows NT 10.0; Win64; x64) AppleWebKit / 537.36(KHTML, like Gecko) Chrome/81.0.4044.129 Safari / 537.36'
    }

    start_time = time.time()
    process_num = 64
    l = []
    temp = pymongo.MongoClient("mongodb://zj184x.corp.youdao.com:30000/")["chat_baike"]
    # db_all = db['triple']
    temp_all = temp['test1']
    
    #import tqdm
    olds = set()
    count = 0
    cursor = temp_all.find({}, {'_id': 1}).batch_size(200)
    #print(1111111111111111)
    for item in cursor:
        olds.add(item['_id'])
        count += 1
        if count % 10000 == 0: 
            print(count)
    #olds = [item['_id'] for item in temp_all.find({}, {'_id': 1})]
    #print(len(olds))
    #olds = set(olds)
    print(len(olds))
    url_list = load()
    record = set()
    length = (len(url_list) // 64)+1

    for i in range(process_num):
        p = CrawlerProcess(request_headers=request_headers, olds=olds, url_list=set(url_list[i*length:(i+1)*length]), record=record, lock=lock)
        p.start()
        l.append(p)
        time.sleep(1)
    for p in l:
        p.join()


    print("time: ", time.time() - start_time)




import requests
import queue
import csv
from threading import Thread
import pandas as pd
from pylab import *
from wordcloud import WordCloud
import matplotlib.pyplot as plt
import jieba
from tqdm import tqdm
import re


class Bilibili:
    def __init__(self, BV, mode, cookies, page):
        self.homeUrl = "https://www.bilibili.com/video/"
        self.oid_get(BV)

        self.replyUrl = "https://api.bilibili.com/x/v2/reply/main?jsonp=jsonp&type=1&oid={oid}&mode={mode}&plat=1&next=".format(
            oid=self.oid, mode=mode)
        self.rreplyUrl = "https://api.bilibili.com/x/v2/reply/reply?jsonp=jsonp&type=1&oid={oid}&ps=20&root={root}&pn=".format(
            oid=self.oid, root="{root}")

        self.headers = {
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.82 Safari/537.36"}
        self.cookies = cookies

        self.q = queue.Queue()
        self.count = 1

    # 获取视频 oid
    def oid_get(self, BV):
        response = requests.get(url=self.homeUrl + BV).text
        # 正则表达式 通过视频 bv 号获取 oid
        self.oid = re.findall("\"aid\":([0-9]*),", response)[0]

    # level_1是否为一级评论
    def content_get(self, url, page, level_1=True):
        now = 0
        while now <= page:
            if level_1:
                print("page : <{now}>/<{page}>".format(now=now, page=page))
            response = requests.get(url=url + str(now), cookies=self.cookies, headers=self.headers).json()
            print(url + str(now))
            replies = response['data']['replies']  # 评论数据在data->replies 里面，一共有 20 条
            now += 1
            for reply in replies:
                if level_1:
                    line = self.reply_clean(reply, self.count)
                    self.count += 1
                else:
                    line = self.reply_clean(reply)
                self.q.put(line)
                # 这儿我们可以筛选一下，如果有二级评论，调用函数请求二级评论
                # if level_1 == True and line[-2] != 0:
                #     self.content_get(url=self.rreplyUrl.format(root=str(line[-1])), page=int(line[-2] / 20 + 0.5),
                #                      level_1=False)  # 递归获取二级评论

    # 这个函数可以爬一级评论也能爬二级评论
    def reply_clean(self, reply, count=False):
        name = reply['member']['uname']  # 名字
        sex = reply['member']['sex']  # 性别
        if sex == "保密":
            sex = ' '
        mid = reply['member']['mid']  # 帐号的uid
        sign = reply['member']['sign']  # 标签
        rpid = reply['rpid']  # 爬二级评论要用到
        rcount = reply['rcount']  # 回复数
        level = reply['member']['level_info']['current_level']  # 等级
        like = reply['like']  # 点赞数
        content = reply['content']['message'].replace("\n", "")  # 评论内容
        t = reply['ctime']
        timeArray = time.localtime(t)
        otherStyleTime = time.strftime("%Y-%m-%d %H:%M:%S", timeArray)  # 评论时间，时间戳转为标准时间格式
        if count:
            return [name, sex, level, mid, sign, otherStyleTime, content, like, rcount, rpid]
        else:
            return [name, sex, level, mid, sign, otherStyleTime, content, like, ' ', rpid]

    def csv_writeIn(self, BV):
        print("csv文件 数据存储中......")
        file = open("bilibili评论_" + BV + ".csv", "w", encoding="utf-8", newline="")
        f = csv.writer(file)
        line1 = ['姓名', '性别', '等级', 'uid', '个性签名', '评论时间', '评论内容', '点赞数', '回复数', 'rpid']
        f.writerow(line1)
        file.flush()

        while True:
            try:
                line = self.q.get(timeout=10)
            except:
                break
            f.writerow(line)
            file.flush()

        file.close()


    def main(self, page,BV):
        #self.mysql_connect(host=host, user=user, password=password, BV=BV)

        T = []
        T.append(Thread(target=self.content_get, args=(self.replyUrl, page)))
        #T.append(Thread(target=self.mysql_writeIn, args=(BV,)))
        T.append(Thread(target=self.csv_writeIn, args=(BV, )))

        print("开始爬取...")
        for t in T:
            t.start()
        for t in T:
            t.join()

class Analysis:
    def __init__(self, data):
        self.data="bilibili评论_BV14h411n7ok.csv"

    def Comment_TopBarh(self,data):
        print("评论TOP20用户图绘制中...")
        # 绘制点赞T20柱状图
        d1 = data.sort_values(by="点赞数", ascending=False).head(20)
        d2 = data.sort_values(by="回复数", ascending=False).head(20)
        x1 = d1["姓名"]
        # x1.reverse()
        y1 = d1["点赞数"]
        plt.barh(x1, y1)
        plt.rcParams['font.sans-serif'] = ['SimHei']
        plt.title("点赞Top20", fontproperties="SimSun")
        plt.ylabel("点赞数", fontproperties="SimSun")
        plt.xlabel("用户", fontproperties="SimSun")
        plt.savefig('./点赞Top20', dpi=400, bbox_inches='tight')
        plt.show()

    def Reply_TopBarh(self,data):
        print("回复TOP20用户图绘制中...")
        d2 = data.sort_values(by="回复数", ascending=False).head(20)
        x2 = d2["姓名"]
        y2 = d2["回复数"]
        plt.barh(x2, y2)
        plt.rcParams['font.sans-serif'] = ['SimHei']
        plt.ylabel("回复数", fontproperties="SimSun")
        plt.xlabel("用户", fontproperties="SimSun")
        plt.title("回复Top20", fontproperties="SimSun")
        plt.savefig('./回复Top20', dpi=400, bbox_inches='tight')
        plt.show()

    # 用户的等级分布饼图
    def Grade_pie(self,data):
        print("用户的等级分布饼图绘制中...")
        d3 = data.等级.value_counts().sort_index(ascending=False)
        pie(d3, labels=['6', '5', '4', '3', '2'],
            explode=(0, 0.2, 0, 0, 0),  # 第二部分突出显示，值越大，距离中心越远
            autopct='%.2f%%', shadow=True
            )
        plt.legend(loc="upper right", fontsize=10, bbox_to_anchor=(1.1, 1.05), borderaxespad=0.3)
        plt.title("用户的等级分布", fontproperties="SimSun")
        plt.savefig('用户的等级分布', dpi=400, bbox_inches='tight')
        show()

    # 性别饼图
    def Gender_pie(self,data):
        print("性别饼图绘制中...")
        d4 = data.性别.value_counts().sort_index(ascending=False)
        pie(d4, labels=['men', 'women', 'secret'],
            explode=(0, 0, 0.2),
            autopct='%.2f%%', shadow=True)

        # 添加图例
        plt.legend(loc="upper right", fontsize=10, bbox_to_anchor=(1.1, 1.05), borderaxespad=0.3)
        plt.title("性别分布", fontproperties="SimSun")
        plt.savefig('性别分布图', dpi=400, bbox_inches='tight')
        show()

    def Comment_time(self,data):
        print("评论数变化图绘制中...")
        freq = '30min'
        # 四舍五入
        data['评论时间'] = pd.to_datetime(data['评论时间'])
        data['评论时间'] = data['评论时间'].dt.round(freq)
        d5 = data.评论时间.value_counts().sort_index(ascending=True).head(60)
        plt.plot(d5)
        plt.ylabel("评论数", fontproperties="SimSun")
        plt.xlabel("评论时间", fontproperties="SimSun")
        plt.title("评论数变化", fontproperties="SimSun")
        plt.savefig('评论数变化折线图', dpi=400, bbox_inches='tight')
        plt.show()

    # 生成词云
    def Comment_wcloud(self,name):
        # 词云
        data = pd.read_csv("bilibili评论_BV14h411n7ok.csv")
        data_list = data.values.tolist()
        filename = "content.txt"
        result = []
        for item in data_list:
            result.append(item[6])
        print("start process {}".format(filename))
        start_time = time.time()
        with open(filename, 'w', encoding='utf-8') as f:
            for item in tqdm(result):
                f.write(item + '\n')
        with open(r"D:\software\pycharm\work\bilibili\content.txt", encoding="utf-8") as file_object:
            contents = file_object.read()
            result = " ".join(jieba.lcut(contents))
        # print(result)  # 每个字符串空一格

        color_mask = imread('g.jpg')  # 建议图片背景颜色为白色
        wordcloud = WordCloud(font_path="msyh.ttc",
                              background_color="white",
                              mask=color_mask)
        """制作词云的步骤三：制作词云"""
        wordcloud.generate(result)  # 加载文本
        wordcloud.to_file("评论词云.png")  # 保存图片
        plt.title("评论词云", fontproperties="SimSun")
        plt.imshow(wordcloud)  # 背景图片
        plt.show()
    def main(self,data):
        data = pd.read_csv("bilibili评论_BV14h411n7ok.csv")
        data.describe()
        data = data.dropna()
        data = data.drop_duplicates()  # 发弹幕总数TOP20的用户柱状图
        self.Comment_TopBarh(data)  # 用户的等级分布饼图
        self.Reply_TopBarh(data)
        self.Grade_pie(data)  # 性别饼图
        self.Gender_pie(data)
        self.Comment_time(data)
        self.Comment_wcloud(data)
if __name__ == '__main__':
    cookie = "_uuid=657210-8EC2-4CC4-9CA7-C1C6B804DF05167640infoc; buvid_fp_plain=F1B301A9-8EC2-4CC4-9CA7-C1C6B804DF05167640infoc; DedeUserID=517144578; DedeUserID__ckMd5=197787e1a943196a; SESSDATA=99fdf911%2C1652767015%2C4bf6d*b1; bili_jct=971a6c010e4f38fc0a92852550a17b; blackside_state=1; rpdid=|(J|~YRu)|JY0J'uYJ~kul|Ru;697ed924ddf787880; bp_video_offset_517144578=612902411653386000; CURRENT_FNVAL=80; CURRENT_BLACKGAP=1; innersign=0; b_lsid=9A129ACD_17E3A5E4588"
    cookie_para = {i.split("=")[0]: i.split("=")[1] for i in cookie.split("; ")}
    BV = 'BV14h411n7ok'
    data = "bilibili评论_BV14h411n7ok.csv"
    bilibili = Bilibili(BV, 0, cookie_para, 100)
    bilibili.main(100, BV)
    Analysis=Analysis(data)
    Analysis.main(data)

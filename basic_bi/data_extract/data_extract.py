# date:2019/3/31
# -*- coding: utf-8 -*-
# auth：Haohao,fuyu

import pymysql
import pandas as pd
import numpy as np
import datetime
import time

from basic_bi.config.basic import *


class extract_data:
    '''
    渠道数据
    '''

    def __init__(self, date=ANALYZE_DATE):
        self.analyze_date = str(date).replace('-', '')
        self.yesterday = str(date - 2*TIME_INTERVAL)
        self.begin_t = str(date - TIME_INTERVAL)
        self.end_t = str(date)

        self.mine_user_action_statistic = 'mine_user_action_' + \
            self.begin_t.replace('-', '')  # 统计日
        self.mine_user_action_yesterday = 'mine_user_action_' + \
            self.yesterday.replace('-', '')  # 统计日前一天，算留存用到
        self.mine_user = 'mine_user'

        self.conn = pymysql.connect(
            host=TMP_DB_HOST,
            port=TMP_DB_PORT, db=TMP_DB, user=TMP_DB_USER, password=TMP_DB_PSWD,
            charset='utf8')
        self.cur = self.conn.cursor()  # 建立游标

        self.conn_online = pymysql.connect(
            host=ONLINE_DB_HOST, port=ONLINE_DB_PORT, user=ONLINE_DB_USER, db=ONLINE_DB, passwd=ONLINE_DB_PSWD)
        self.cursor_online = conn_online.cursor()

        self.is_get_tmp_tables = False
        self.is_get_new_accu_user_action = False
        self.is_get_date_user_info = False
        self.is_tmp_accu_user_info = False

    def _toDataFrame(self, dat):
        return pd.DataFrame(list(dat))

    def stop_conn(self):
        self.cur.close()
        self.conn.close()
        self.conn_online.close()
        self.cursor_online.close()
    
    def start_init(self):
        '''
        标准启动方法()
        '''
        self.get_tmp_tables()
        self.get_new_accu_user_action()
        self.get_date_user_info()
        self.display_accu_info()
        self.display_active_info()
        self.display_new_info()
        self.display_stay_info()

    def get_tmp_tables(self):
        '''
        累积总激活用户数，总绑定微信用户数(从8号开始才有channel字段)
        '''

        sql = """
        insert into  tmp_new_accu_user_action
        select distinct a.uid,a.channel,a.device_id,a.created_at
        from %s a
        inner join(
            select uid,min(created_at) created_at -- 确保一个uid对应一个最早时间
            from %s
            where uid <> 0 and channel <> ''
            group by uid
        )b
        on a.uid = b.uid and a.created_at = b.created_at
        where a.uid <> 0 and a.channel <> '' ;-- 统计日
        """ % (self.mine_user_action_statistic, self.mine_user_action_statistic)

        self.cur.execute(sql)
        self.is_get_tmp_tables = True

    @get_tmp_tables
    def get_new_accu_user_action(self):
        if not self.is_get_tmp_tables:
            self.get_tmp_tables()
        sql = """drop table if exists new_accu_user_action;"""
        self.cur.execute(sql)

        sql = """
        create table new_accu_user_action as
        select distinct a.uid,a.channel,a.device_id,a.created_at
        from tmp_new_accu_user_action a
        inner join(
            select uid,min(created_at) created_at -- 确保一个uid对应一个最早时间
            from tmp_new_accu_user_action
            group by uid
        )b
        on a.uid = b.uid and a.created_at = b.created_at;
        """
        self.cur.execute(sql)
        self.is_get_new_accu_user_action = True

    @get_new_accu_user_action
    def get_date_user_info(self):
        if not self.is_get_tmp_tables:
            self.get_tmp_tables()
        sql = """drop table if exists today_user_info;"""
        self.cur.execute(sql)

        # 今日活跃的用户信息
        sql = """
            create table today_user_info as 
            select a.device_id,a.uid,u.wechat_id,a.created_at,
            a.channel,u.nickname,u.inviter_uid,u.alipay_id,u.inviter_my_code,
            case when u.create_time BETWEEN UNIX_TIMESTAMP('%s') and UNIX_TIMESTAMP('%s')
                and a.uid is not null then 'y' else 'n' end as isNewer 
            from (
            select * 
            from tmp_new_accu_user_action
            WHERE uid > 0 and channel <> '' 
            and created_at BETWEEN '%s' AND '%s') a 
            LEFT JOIN mine_user u 
            on a.uid = u.uid;
        """ % (begin_t, end_t, begin_t, end_t)
        self.cur.execute(sql)

        sql = """select * from today_user_info;"""
        self.cur.execute(sql)
        res = self.cur.fetchall()  # 获取结果
        today_user = self._toDataFrame(res)
        today_user.columns = ['device_id', 'uid', 'wechat_id', 'created_at', 'channel',
                              'nickname', 'inviter_my_code', 'alipay_id', 'inviter_uid', 'isNewer']

        sql = """drop table if exists yesterday_user_info;"""
        self.cur.execute(sql)
        # 昨日活跃的用户信息

        sql = """
            create table yesterday_user_info as 
            select a.device_id,a.uid,u.wechat_id,a.created_at,
            a.channel,u.nickname,u.inviter_uid,u.alipay_id,u.inviter_my_code,
            case when u.create_time BETWEEN UNIX_TIMESTAMP('%s') and UNIX_TIMESTAMP('%s')
                and a.uid is not null then 'y' else 'n' end as isNewer 
            from (
            select * 
            from tmp_new_accu_user_action
            WHERE uid > 0 and channel <> '' 
            and created_at BETWEEN '%s' AND '%s') a 
            LEFT JOIN mine_user u on a.uid = u.uid;
        """ % (yesterday, begin_t, yesterday, begin_t)
        self.cur.execute(sql)

        # 加索引
        sql = """ALTER TABLE today_user_info ADD INDEX index_wechat_id (wechat_id);"""
        self.cur.execute(sql)

        sql = """ALTER TABLE today_user_info ADD INDEX index_uid (uid);"""
        self.cur.execute(sql)

        sql = """ALTER TABLE yesterday_user_info ADD INDEX index_wechat_id (wechat_id);"""
        self.cur.execute(sql)

        sql = """ALTER TABLE yesterday_user_info ADD INDEX index_uid (uid);"""
        self.cur.execute(sql)

        self.is_get_date_user_info = True

    def display_accu_info(self):
        '''
        统计累计信息
        '''
        sql = """
        select b.channel, 
           count(distinct b.device_id) device_id_num,  
           count(distinct a.uid) uid_num, 
           count(distinct case when length(a.wechat_id)>1 then b.device_id else null end)  wechat_device_num, 
           count(distinct case when length(a.wechat_id)>1 then a.wechat_id else null end)  wechat_id_num ,   
           count(distinct case when length(a.alipay_id)>1 then a.alipay_id else null end)  alipay_id_num    
           from %s a 
           inner join new_accu_user_action b 
           on a.uid = b.uid 
           where a.create_time BETWEEN UNIX_TIMESTAMP('2019-01-08') and UNIX_TIMESTAMP('%s') 
           group by b.channel 
           order by b.channel; 
        """ % (self.mine_user, self.analyze_date)
        self.cur.execute(sql)

        res = self.cur.fetchall()  # 获取结果
        self.accu_user_related = self._toDataFrame(res)
        self.accu_user_related.columns = [
            '渠道', '累计设备数', '累计uid数', '累计绑定微信设备数', '累计微信账号数', '累计支付宝账号数']

        # 邀请数
        sql = "drop table if exists tmp_accu_user_info;"
        self.cur.execute(sql)

        # 得到邀请人和被邀请者的信息，主体是邀请人，统计邀请人的信息（id,电话，来源渠道，邀请码，建立时间，邀请该下级的时间距今时长），及这些邀请人所邀请的被邀请者的信息（id,电话，微信，设备号）
        # 候选键：邀请人uid,被邀请者uid
        sql = """
        create table tmp_accu_user_info as 
        select  
        a.uid,
        a.mobile,
        c.channel,
        a.inviter_my_code,
        date(FROM_UNIXTIME(a.create_time)) create_time,
        DATEDIFF('%s',date(FROM_UNIXTIME(b.create_time))) xiaji_diff, -- 下级创建时间据统计时间的差
        b.uid xiaji_uid,
        b.mobile xiaji_mobile,
        b.wechat_id xiaji_wechat_id,
        d.device_id xiaji_device_id
            from mine_user a 
            inner join (select distinct uid,mobile,create_time,inviter_uid,wechat_id
                        from  mine_user 
                        where create_time < UNIX_TIMESTAMP('%s') ) b -- 提供下级信息
            on a.uid = b.inviter_uid
            inner join (select distinct uid,channel from new_accu_user_action )  c -- 用来附加上级渠道字段
            on a.uid = c.uid
            inner join (select distinct uid,device_id from new_accu_user_action ) d -- 用来附加下级设备字段
            on b.uid = d.uid;
        """ % (self.begin_t, self.end_t)
        self.cur.execute(sql)

        sql = """
        select 
        channel,
        count(distinct case when length(xiaji_wechat_id) > 1 then xiaji_device_id else null end) xiaji_wechat_id_num,  -- 渠道下级数（这个渠道的人发展的下级绑定了微信的设备数）
        count(distinct xiaji_device_id) xiaji_device_id_num                                                 -- 渠道下级数（这个渠道的人发展的下级设备数）
        from tmp_accu_user_info
        group by channel
        order by channel;
        """
        self.cur.execute(sql)

        res = self.cur.fetchall()  # 获取结果
        accu_user_invited = self._toDataFrame(res)
        accu_user_invited.columns = ['渠道', '渠道徒弟数_绑定微信设备', '渠道徒弟数_设备数']

        # 完成第一天邀请奖励的下级数
        sql = """
        select channel,
        count(distinct case when length(xiaji_wechat_id) > 1 then xiaji_device_id else null end) xiaji_wechat_id_num,  -- 渠道下级数（这个渠道的人发展的下级绑定了微信的设备数）
        count(distinct xiaji_device_id) xiaji_device_id_num
        from tmp_accu_user_info a
        where xiaji_uid in (
            select distinct uid
            from mine_user_account_mibi_log 
            where extend = '⾸次邀请好友'
        )
        group by channel;
        """
        self.cur.execute(sql)
        res = self.cur.fetchall()  # 获取结果
        first_user_invited = self._toDataFrame(res)
        first_user_invited.columns = [
            '渠道', '渠道徒弟完成第一天邀请奖励_绑定微信设备数', '渠道徒弟完成第一天邀请奖励_设备数']

        sql = """
        select b.channel,
        sum(case when a.money=1 then a.money else 0 end) totoal_one_cash_sum , -- 累计一元提现
        sum(case when a.money is not null then a.money else 0 end) totoal_cash_sum  -- 累计提现
        from mine_withdraw a
        inner join (
            select distinct uid,channel 
            from new_accu_user_action
            where created_at BETWEEN '2019-01-08' and '%s'   -- 第二个时间为今天
            ) b 
        on a.uid = b.uid
        and b.channel <> '' and b.uid>0
        group by b.channel
        order by b.channel;
        """ % (self.begin_t)
        self.cur.execute(sql)
        res = self.cur.fetchall()  # 获取结果
        accu_user_cash = self._toDataFrame(res)
        accu_user_cash.columns = ['渠道', '累计一元提现', '累计总提现']
        result = pd.merge(self.accu_user_related,
                          accu_user_invited, on='渠道', how='outer')
        result = pd.merge(result, first_user_invited, on='渠道', how='outer')
        self.accu_user_related = pd.merge(
            result, accu_user_cash, on='渠道', how='outer')

        self.is_tmp_accu_user_info = True

    def display_active_info(self):
        sql = """
        select channel,
        count(distinct device_id) device_id_num,  -- 总device_id数
        count(distinct uid) uid_num,  -- 总uid数
        count(distinct case when length(wechat_id)>1 then device_id else null end)  wechat_device_num,  -- 总绑定微信设备数
        count(distinct case when length(wechat_id)>1 then wechat_id else null end)  wechat_id_num,  -- 总微信账户
        count(distinct case when length(alipay_id)>1 then alipay_id else null end)  alipay_id_num    -- 总支付宝账户
        from today_user_info
        group by channel
        order by channel;
        """

        self.cur.execute(sql)
        res = self.cur.fetchall()  # 获取结果
        tmp1 = self._toDataFrame(res)
        tmp1.columns = ['渠道', '设备数', 'uid数', '绑定微信设备数', '微信账号数', '支付宝账号数']

        # 当日刷新，点击，阅读
        chars1 = '%refresh=1%'
        chars2 = '%refresh=2%'
        chars3 = '%feedback?type=3%'
        chars3_1 = '%feedback?type=click%'
        chars3_2 = '%video&type=click%'

        sql = """
        select t.channel,
        sum(case when t.url like '%s' or url  like '%s' then 1 else 0 end) refresh_num,  -- 总刷新次数
        sum(case when t.url like '%s' or t.url like '%s'  or t.url like '%s' then 1 else 0 end) click_num
        from 
        (select  uid,channel,url from %s where channel <> '' and uid>0)t
        group by t.channel
        order by t.channel
        """ % (chars1, chars2, chars3, chars3_1, chars3_2, self.mine_user_action_statistic)

        self.cur.execute(sql)

        res = self.cur.fetchall()  # 获取结果
        tmp2_1 = self._toDataFrame(res)
        tmp2_1.columns = ['渠道', '刷新次数', '点击次数']

        print('****************************444444')

        # 将用户行为表(uid,channel）与获取蜜币的行为表（uid,extend=！不同行为全看做阅读的不同文章？,type=type为1的事件为阅读）结合（获取蜜币就是阅读的行为），获取渠道阅读log
        sql = """
        SELECT t.channel,
        count(distinct  t.extend ) readed_news_num, -- 总阅读文章数
        sum(case when t.type=1 then 0.5 else 0 end) reading_time_num -- 总阅读时长
        from 
        (
            select distinct a.uid,a.channel,m.extend,m.type
            from
            (select distinct uid,channel from %s where channel <> '' and uid>0)a
            inner join 
            (select uid,extend,type from %s where type=1 and date(created_at) = '%s') m
            on a.uid = m.uid
        )t
        group by t.channel
        order by t.channel;
        """ % (self.mine_user_action_statistic, 'mine_user_account_mibi_log', self.begin_t)

        self.cur.execute(sql)

        res = self.cur.fetchall()  # 获取结果
        tmp2_2 = self._toDataFrame(res)
        tmp2_2.columns = ['渠道', '阅读文章数', '阅读时长']

        print('****************************55555')

        # 当日任务相关

        sql = """
        select m.channel,
        count(case when m.type = 1 then m.type else null end) newer_task_num, -- 总完成新手任务数
        count(distinct case when m.type = 1 then m.device_id else null end) newer_task_device_id_num, -- 总完成新手任务的设备数
        count(distinct case when m.type = 1 then m.wechat_id else null end) newer_task_wechat_id_num, -- 总完成新手任务的微信数
        count(case when m.type = 2 then m.type else null end) daily_task_num,  -- 总完成日常任务数
        count(distinct case when m.type = 2 then m.device_id else null end) daily_task_device_id_num, -- 总完成日常任务的设备数
        count(distinct case when m.type = 2 then m.wechat_id else null end) daily_task_wechat_id_num -- 总完成日常任务的微信数
        from(
        select distinct a.channel,a.device_id,a.wechat_id,t.type
        from today_user_info a
        inner join (select  uid,type from mine_task_system_logs where date(created_at) = '%s' ) t
        on a.uid = t.uid
        where a.channel <> '' and a.uid>0
        ) m
        group by m.channel
        order by m.channel;
        """ % (self.begin_t)

        self.cur.execute(sql)

        res = self.cur.fetchall()  # 获取结果
        tmp3 = self._toDataFrame(res)
        tmp3.columns = ['渠道', '完成新手任务数', '完成新手任务设备数', '完成新手任务微信数',
                        '完成日常任务数', '完成日常任务设备数', '完成日常任务微信数']

        print('****************************666666')
        # 当日提现相关

        sql = """
        select t.channel,
        count(case when t.money = 1 then t.money else null end) daily_task_num,  -- 当日发起一元提现的数量
        count(distinct case when t.money = 1 then t.device_id else null end) daily_task_device_id_num, -- 当日发起一元提现设备数
        count(distinct case when t.money = 1 then t.wechat_id else null end) daily_task_wechat_id_num, -- 当日发起一元提现的微信账号数
        count(distinct case when t.money is not null then t.device_id else null end) cash_device_num , -- 当日发起提现的设备
        count(distinct case when t.money is not null then t.wechat_id else null end) cash_wechat_id_num  -- 当日发起提现的微信号
        from(
        select distinct a.channel,a.device_id,a.wechat_id, money 
        from today_user_info a
        inner join (select uid,money from mine_withdraw where  date(created_at) = '%s' ) w
        on a.uid = w.uid
        where a.channel <> '' and a.uid>0
        )t
        group by t.channel
        order by t.channel;
        """ % (self.begin_t)

        self.cur.execute(sql)

        res = self.cur.fetchall()  # 获取结果
        print(res)
        tmp4 = self._toDataFrame(res)

        tmp4.columns = ['渠道', '发起一元提现数', '发起一元提现设备数', '发起一元提现微信数',
                        '发起提现设备数', '发起提现微信数']

        sql = """
            select t.channel,
            sum(case when t.money is not null then t.money else 0 end) totoal_cash_sum -- 当日总提现金额
            from(
            select  a.channel,money 
            from today_user_info a
            inner join (select uid,money from mine_withdraw where  date(created_at) = '%s' ) w
            on a.uid = w.uid
            where a.channel <> '' and a.uid>0
            )t
            group by t.channel
            order by t.channel;
        """ % (begin_t)

        self.cur.execute(sql)

        res = self.cur.fetchall()  # 获取结果
        tmp5 = self._toDataFrame(res)
        tmp5.columns = ['渠道', '提现总金额']

        print('****************************77777')

        result = pd.merge(tmp1, tmp2_1, on='渠道', how='outer')
        result = pd.merge(result, tmp2_2, on='渠道', how='outer')
        result = pd.merge(result, tmp3, on='渠道', how='outer')
        result = pd.merge(result, tmp4, on='渠道', how='outer')
        curr_user_related = pd.merge(result, tmp5, on='渠道', how='outer')
        curr_user_related.fillna(0, inplace=True)

        curr_user_related[['阅读时长']] = curr_user_related[['阅读时长']].astype(float)
        curr_user_related[['提现总金额']
                          ] = curr_user_related[['提现总金额']].astype(float)
        curr_user_related[['刷新次数']] = curr_user_related[['刷新次数']].astype(float)
        curr_user_related[['点击次数']] = curr_user_related[['点击次数']].astype(float)

        curr_user_related.eval('平均刷新次数_设备 = 刷新次数/设备数', inplace=True)
        curr_user_related.eval('平均点击次数_设备 = 点击次数/设备数', inplace=True)
        curr_user_related.eval('平均阅读文章数_设备 = 阅读文章数 /设备数', inplace=True)
        curr_user_related.eval('平均阅读时长_设备_分钟 = 阅读时长 /设备数', inplace=True)
        curr_user_related.eval('平均完成新手任务数_设备 = 完成新手任务数 /设备数', inplace=True)
        curr_user_related.eval('平均完成日常任务数_设备 = 完成日常任务数 /设备数', inplace=True)
        curr_user_related.eval('平均发起一元提现数_设备 = 发起一元提现数 /设备数', inplace=True)
        curr_user_related.eval('平均提现数_设备 = 提现总金额/设备数', inplace=True)

        curr_user_related.eval('平均刷新次数_微信 = 刷新次数/微信账号数', inplace=True)
        curr_user_related.eval('平均点击次数_微信 = 点击次数/微信账号数', inplace=True)
        curr_user_related.eval('平均阅读文章数_微信 = 阅读文章数 /微信账号数', inplace=True)
        curr_user_related.eval('平均阅读时长_微信_分钟 = 阅读时长 /微信账号数', inplace=True)
        curr_user_related.eval('平均完成新手任务数_微信 = 完成新手任务数 /微信账号数', inplace=True)
        curr_user_related.eval('平均完成日常任务数_微信 = 完成日常任务数 /微信账号数', inplace=True)
        curr_user_related.eval('平均发起一元提现数_微信 = 发起一元提现数 /微信账号数', inplace=True)
        curr_user_related.eval('平均提现数_微信 = 提现总金额/微信账号数', inplace=True)
        self.curr_user_related = curr_user_related

        print('****************************88888')

        # 合并累计和当日活跃的，便于比较
        self.acc_curr_related = pd.merge(
            self.accu_user_related, curr_user_related, on='渠道', how='outer')

    def display_new_info(self):
        sql = """
        select channel,
        count(distinct device_id) device_id_num,  -- 总device_id数
        count(distinct uid) uid_num,  -- 总uid数
        count(distinct case when length(wechat_id)>1 then device_id else null end)  wechat_device_num,  -- 总绑定微信设备数
        count(distinct case when length(wechat_id)>1 then wechat_id else null end)  wechat_id_num,  -- 总微信账户
        count(distinct case when length(alipay_id)>1 then alipay_id else null end)  alipay_id_num    -- 总支付宝账户
        from today_user_info
        where isNewer = 'y'
        group by channel
        order by channel;
        """

        self.cur.execute(sql)

        res = self.cur.fetchall()  # 获取结果
        tmp_n1 = self._toDataFrame(res)
        tmp_n1.columns = ['渠道', '设备数', 'uid数', '绑定微信设备数', '微信账号数', '支付宝账号数']
        print(tmp_n1)
        print('****************************99999')

        # 新增对象当日刷新，点击，阅读
        chars1 = '%refresh=1%'
        chars2 = '%refresh=2%'
        chars3 = '%feedback?type=3%'
        chars3_1 = '%feedback?type=click%'
        chars3_2 = '%video&type=click%'

        sql = """drop table if exists tmp_newer_info;"""
        self.cur.execute(sql)

        sql = """
        create table tmp_newer_info as
        select distinct a.uid,b.channel,b.url
        from 
        (select uid from mine_user where create_time BETWEEN UNIX_TIMESTAMP('%s') and UNIX_TIMESTAMP('%s') )a
        inner join 
        (select distinct uid,channel,url from %s where channel <> '' and uid>0)b
        on a.uid = b.uid;
        """ % (self.begin_t, self.end_t, self.mine_user_action_statistic)
        self.cur.execute(sql)

        sql = """
        select t.channel,
        sum(case when t.url like '%s' or t.url  like '%s' then 1 else 0 end) refresh_num,  -- 总刷新次数
        sum(case when t.url like '%s' or t.url like '%s' or t.url like '%s' then 1 else 0 end) click_num   -- 总点击次数
        from %s t 
        inner join (select distinct uid from tmp_newer_info) b
        on t.uid = b.uid
        group by t.channel
        order by t.channel; 
        """ % (chars1, chars2, chars3, chars3_1, chars3_2, self.mine_user_action_statistic)

        self.cur.execute(sql)

        res = self.cur.fetchall()  # 获取结果
        tmp_n2_1 = self._toDataFrame(res)

        tmp_n2_1.columns = ['渠道', '刷新次数', '点击次数']

        print('****************************10')

        sql = """
        SELECT t.channel,
        count(distinct  t.extend ) readed_news_num, -- 总阅读文章数
        sum(case when t.type=1 then 0.5 else 0 end) reading_time_num -- 总阅读时长
        from 
        (
            select distinct a.uid,a.channel,m.extend,m.type
            from
            (select distinct uid,channel from tmp_newer_info )a
            inner join 
            (select uid,extend,type from %s where type=1 and date(created_at) = '%s') m
            on a.uid = m.uid
        )t
        group by t.channel
        order by t.channel; 
        """ % (self.mine_user_account_mibi_log, self.begin_t)

        self.cur.execute(sql)

        res = self.cur.fetchall()  # 获取结果
        tmp_n2_2 = self._toDataFrame(res)

        tmp_n2_2.columns = ['渠道', '阅读文章数', '阅读时长']

        print('****************************11')

        # 新增对象当日任务相关

        sql = """
        select m.channel,
        count(case when m.type = 1 then m.type else null end) newer_task_num, -- 总完成新手任务数
        count(distinct case when m.type = 1 then m.device_id else null end) newer_task_device_id_num, -- 总完成新手任务的设备数
        count(distinct case when m.type = 1 then m.wechat_id else null end) newer_task_wechat_id_num, -- 总完成新手任务的微信数
        count(case when m.type = 2 then m.type else null end) daily_task_num,  -- 总完成日常任务数
        count(distinct case when m.type = 2 then m.device_id else null end) daily_task_device_id_num, -- 总完成日常任务的设备数
        count(distinct case when m.type = 2 then m.wechat_id else null end) daily_task_wechat_id_num -- 总完成日常任务的微信数
        from(
        select distinct a.channel,a.device_id,a.wechat_id,t.type
        from today_user_info a
        inner join (select uid,type from mine_task_system_logs where date(created_at) = '%s' ) t
        on a.uid = t.uid
        where a.channel <> '' and a.uid>0
        and a.isNewer = 'y'
        ) m
        group by m.channel
        order by m.channel;
        """ % (self.begin_t)

        self.cur.execute(sql)

        res = self.cur.fetchall()  # 获取结果
        tmp_n3 = self._toDataFrame(res)
        tmp_n3.columns = ['渠道', '完成新手任务数', '完成新手任务设备数', '完成新手任务微信数',
                          '完成日常任务数', '完成日常任务设备数', '完成日常任务微信数']

        print('****************************12')

        # 新增渠道下级

        sql = """
        select 
        channel,
        count(distinct case when xiaji_diff=0 and length(xiaji_wechat_id) > 1 then xiaji_device_id else null end)  new_xiaji_wechat_device_id_num, -- 新增渠道下级数（这个渠道的人发展的下级绑定了微信的设备数）
        count(distinct case when xiaji_diff=0 then xiaji_device_id else null end ) new_xiaji_device_id_num  -- 新增渠道下级数（这个渠道的人发展的下级设备数）
        from tmp_accu_user_info
        group by channel
        order by channel;
        """
        self.cur.execute(sql)

        res = self.cur.fetchall()  # 获取结果
        tmp6 = self._toDataFrame(res)
        tmp6.columns = ['渠道', '新增_渠道下级数（绑定微信设备数）', '新增_渠道下级数（设备数）']

        print('****************************13')

        result = pd.merge(tmp_n1, tmp_n2_1, on='渠道', how='outer')
        result = pd.merge(result, tmp_n2_2, on='渠道', how='outer')
        result = pd.merge(result, tmp_n3, on='渠道', how='outer')
        today_New_user_related = pd.merge(result, tmp6, on='渠道', how='outer')

        today_New_user_related[['阅读时长']
                               ] = today_New_user_related[['阅读时长']].astype(float)
        today_New_user_related[['刷新次数']
                               ] = today_New_user_related[['刷新次数']].astype(float)
        today_New_user_related[['点击次数']
                               ] = today_New_user_related[['点击次数']].astype(float)

        today_New_user_related.eval('平均刷新次数_设备 = 刷新次数/设备数', inplace=True)
        today_New_user_related.eval('平均点击次数_设备 = 点击次数/设备数', inplace=True)
        today_New_user_related.eval('平均阅读文章数_设备 = 阅读文章数 /设备数', inplace=True)
        today_New_user_related.eval('平均阅读时长_设备_分钟 = 阅读时长 /设备数', inplace=True)
        today_New_user_related.eval(
            '平均完成新手任务数_设备 = 完成新手任务数 /设备数', inplace=True)
        today_New_user_related.eval(
            '平均完成日常任务数_设备 = 完成日常任务数 /设备数', inplace=True)

        today_New_user_related.eval('平均刷新次数_微信 = 刷新次数/微信账号数', inplace=True)
        today_New_user_related.eval('平均点击次数_微信 = 点击次数/微信账号数', inplace=True)
        today_New_user_related.eval('平均阅读文章数_微信 = 阅读文章数 /微信账号数', inplace=True)
        today_New_user_related.eval('平均阅读时长_微信_分钟 = 阅读时长 /微信账号数', inplace=True)
        today_New_user_related.eval(
            '平均完成新手任务数_微信 = 完成新手任务数 /微信账号数', inplace=True)
        today_New_user_related.eval(
            '平均完成日常任务数_微信 = 完成日常任务数 /微信账号数', inplace=True)
        self.today_New_user_related = today_New_user_related

    def display_stay_info(self):
        chosen_date = begin_t.replace('-', '')
        sql = """
        select channel,y_device_id_num,
        y_uid_num,y_wechat_device_num,
        y_wechat_id_num,
        device_id_num,uid_num ,device_wechat_id_num ,wechat_id_num 
        from fuyu_result_dayone_retention
        where pt = '%s' and type = 'active'
        """ % (chosen_date)

        self.cur.execute(sql)

        res = self.cur.fetchall()  # 获取结果
        active_remain_related = self._toDataFrame(res)
        active_remain_related.columns = ['渠道', '昨日设备数', '昨日uid数', '昨日绑定微信设备数', '昨日微信账号数',
                                         '设备数', 'uid数', '绑定微信设备数', '微信账号数']

        print('****************************15')

        active_remain_related.fillna(0, inplace=True)
        active_remain_related.eval('设备留存比例 = 设备数/昨日设备数*100', inplace=True)
        active_remain_related.eval('uid留存比例 = uid数/昨日uid数*100', inplace=True)
        active_remain_related.eval(
            '绑定微信设备数留存比例 = 绑定微信设备数 /昨日绑定微信设备数*100', inplace=True)
        active_remain_related.eval(
            '微信账号数留存比例 = 微信账号数 /昨日微信账号数*100', inplace=True)
        active_remain_related.fillna(0, inplace=True)

        print('****************************16')

        # (二)新增

        start_time = int(time.time())
        sql = """
        select channel,y_device_id_num,
        y_uid_num,y_wechat_device_num,
        y_wechat_id_num,
        device_id_num ,uid_num,device_wechat_id_num ,wechat_id_num 
        from fuyu_result_dayone_retention
        where pt = '%s' and type = 'new'
        """ % (chosen_date)

        self.cur.execute(sql)

        res = self.cur.fetchall()  # 获取结果
        new_remain_related = self._toDataFrame(res)
        new_remain_related.columns = ['渠道', '昨日设备数', '昨日uid数', '昨日绑定微信设备数', '昨日微信账号数',
                                      '设备数', 'uid数', '绑定微信设备数', '微信账号数']

        print('****************************17')

        new_remain_related.fillna(0, inplace=True)
        new_remain_related.eval('设备留存比例 = 设备数/昨日设备数*100', inplace=True)
        new_remain_related.eval('uid留存比例 = uid数/昨日uid数*100', inplace=True)
        new_remain_related.eval(
            '绑定微信设备数留存比例 = 绑定微信设备数 /昨日绑定微信设备数*100', inplace=True)
        new_remain_related.eval('微信账号数留存比例 = 微信账号数 /昨日微信账号数*100', inplace=True)

        new_remain_related.fillna(0, inplace=True)

        # 将两个表格输出到一个excel文件里面
        writer = pd.ExcelWriter(
            '/root/fuyu/channel_data/'+self.analyze_date+'.xlsx')
        self.accu_user_related.to_excel(writer, sheet_name='累计数据')
        self.curr_user_related.to_excel(writer, sheet_name='当日活跃用户')
        self.acc_curr_related.to_excel(writer, sheet_name='累计及当日活跃用户')
        self.today_New_user_related.to_excel(writer, sheet_name='今日新增相关')
        active_remain_related.round(2).to_excel(
            writer, sheet_name=yesterday+'活跃次日留存相关')
        new_remain_related.round(2).to_excel(
            writer, sheet_name=yesterday+'新增对象次日留存相关')

        # 必须运行writer.save()，不然不能输出到本地

        writer.save()

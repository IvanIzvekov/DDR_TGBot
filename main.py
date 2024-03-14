import psycopg2
from datetime import date, timedelta
import asyncio
import logging
from aiogram import Bot, Dispatcher
import aiohttp
from env import *
from connectors import Connectors


class DrrCalculator:
    def __init__(self):
        self.companies = None
        self.targeting_cost = []
        self.sales = []
        self.sales_total = {}
        self.high_drr = []
        self.connectors = Connectors()

        self.select_data()
        self.prepare_data()
        self.drr_calc()
        self.select_company_name()

    def select_data(self):

        self.connectors.connect()

        today_date = date.today()
        yesterday_date = date.today() - timedelta(days=1)

        query = f'''SELECT nmid, SUM(sum), company_id FROM {DB_SCHEMA}.advert_fullstat 
        WHERE date >= '{yesterday_date}'::date AND date < '{today_date}'::date AND apps <> 32 AND apps <> 64 
        GROUP BY nmid, company_id;'''

        self.targeting_cost = self.connectors.execute_sql(query)

        query = f'''SELECT nmid, totalprice, discountpercent, company_id FROM {DB_SCHEMA}.orders
            WHERE date >= '{yesterday_date}'::date AND date < '{today_date}'::date AND ordertype = 'Клиентский';'''

        self.sales = self.connectors.execute_sql(query)
        self.connectors.close()

    def prepare_data(self):
        for row in self.sales:
            if row[0] in self.sales_total:
                self.sales_total[row[0]] += (row[1] * ((100 - row[2]) / 100)) * (1 - 0.25)
            else:
                self.sales_total[row[0]] = (row[1] * ((100 - row[2]) / 100)) * (1 - 0.25)

    def drr_calc(self):
        self.high_drr = []
        for row in self.targeting_cost:
            if str(row[0]) in self.sales_total:
                if row[1] >= (self.sales_total[str(row[0])] / 100) * DRR_VALUE:
                    self.high_drr.append([row[0], row[2]])
            # 100%
            else:
                self.high_drr.append([row[0], row[2]])

        if len(self.high_drr) > 0:
            self.high_drr = sorted(self.high_drr, reverse=False, key=lambda x: x[1])

    @staticmethod
    def parse_company_name(name):
        name = str(name)
        name = name.replace('ООО', '').replace('ИП', '').replace(' ', '')
        name = name.replace('ПРАВОВОЙЦЕНТР', '')
        if '.' in name:
            name = name[0:len(name) - 4]
        return name

    def select_company_name(self):
        self.connectors.connect()
        query = f'''SELECT id, name FROM {DB_SCHEMA}.companies;'''
        self.companies = self.connectors.execute_sql(query)
        self.connectors.close()

    def get_company_name_from_id(self, id_company):
        for item in self.companies:
            if item[0] == id_company:
                return self.parse_company_name(item[1])


async def cmd_start(bot: Bot):
    answer = ""
    try:
        drr = DrrCalculator()
        if len(drr.high_drr) > 0:
            current_company = -1
            for item in drr.high_drr:
                if item[1] != current_company and current_company != -1:
                    current_company = -1
                    await bot.send_message(TG_CHANNEL, answer)
                    answer = ""
                if current_company == -1:
                    answer += "#" + drr.get_company_name_from_id(item[1]) + "\n"
                    current_company = item[1]
                if current_company == item[1]:
                    answer += "Обрати внимание!!! на SKU " + str(item[0]) + " ДРР выше 5%\n"
            await bot.send_message(TG_CHANNEL, answer)
    except(Exception, psycopg2.Error) as error:
        print(error)
        answer = "Что-то пошло не так! Сервер не отвечает!"
        await bot.send_message(TG_CHANNEL, answer)


async def main():
    logging.basicConfig(level=logging.INFO)
    bot = Bot(token=BOT_TOKEN)
    dp = Dispatcher()
    await cmd_start(bot)


if __name__ == "__main__":
    asyncio.run(main())

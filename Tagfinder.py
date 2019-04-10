#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Sep  4 10:34:00 2018

@author: sowmiyanmorri
"""

import re
import datetime
from datetime import date
import pandas as pd
from fuzzywuzzy import fuzz
from dateutil.relativedelta import relativedelta


def replace_all(text, dic):
    for i in dic:
        text = text.replace(i, "")
    return text


def remwrolling(text, there=re.compile(re.escape('rolling') + '.*')):
    return there.sub('', text)


def remwlast(text, there=re.compile(re.escape('last') + '.*')):
    return there.sub('', text)


def item_finder(question_ask):
    print("Item loop")
    dict_columns = pd.read_csv("Sample.csv")
    products = dict_columns["SC Business Segment"].tolist() + \
               dict_columns["SC Modality"].tolist() + \
               dict_columns["SC Product"].tolist() + \
               dict_columns["SC Product Segment"].tolist()
    products = list(set(products))
    del products[0]
    question = question_ask
    question = question.lower()
    question = remwrolling(question)
    question = remwlast(question)

    question = question.replace("months", "month")
    question = question.replace("years", "year")
    question = question.replace("quarters", "quarter")
    question = question.replace("weeks", "week")
    question = question.replace("january", "jan")
    question = question.replace("february", "feb")
    question = question.replace("march", "mar")
    question = question.replace("april", "apr")
    question = question.replace("july", "jul")
    question = question.replace("august", "aug")
    question = question.replace("september", "sep")
    question = question.replace("october", "oct")
    question = question.replace("november", "nov")
    question = question.replace("december", "dec")

    dic = {"on time install", "oti", "last", "on-time install", "% on-time install",
           "% on time install", "% install",
           "on time delivery",
           "otd", "on-time delivery", '% on-time delivery', '% on-time delivery', '% delivery',
           "fmi", "recall total cost", 'recalls', 'recall',
           'number of recalls',
           "complaint rate", "complaint/ib", "complaints/ib",
           'complaints/install base', 'complaints',
           "obf", "out of box failure", "out of box", "out of box failures",
           "elf", "early life failure”, “early life failures", "elf 90", "elf90",
           "ifr", "intial failure rate", "ifr 90", "ifr90",
           "warranty per unit", "wpu",
           "gsc scrap", "scrap", "gsc",
           "cso", "open red cso", "open red", "# open red > 90 days", "red cso", "red csos",
           "cso total", "total cso", "open cso", "# of total csos", "# of total cso",
           "cost to serve", "cts", "cost to serve on contract",
           "foi", 'failure on install', 'install failure rate',
           'for', 'available',
           'is', 'show', 'trend', 'year', 'month', "show", "week",
           'quarter', 'from', 'to', 'check', 'product', 'modality',
           'business', "and", "2017", "2018",
           "jan", "feb", "mar", "apr", "may", "june", "jul", "aug",
           "sep", "oct", "nov", "dec", "ytd", "rolling", "2019", "me"}

    question = replace_all(question, dic)
    question = question.replace("  ", "")
    question = question.upper()

    if "from" in question:
        question = re.search('(.*)(?:from)', question).group(1)
    print(question)

    matches = [(i, fuzz.ratio(question, i)) for i in products]

    if (max(matches, key=lambda item: item[1]))[1] <= 60:
        item = "GEHC"
    else:
        item = max(matches, key=lambda item: item[1])[0]
    return item


def previous_quarter(ref):
    """Getting quarter information"""
    if ref.month < 4:
        return datetime.date(ref.year - 1, 12, 1)
    elif ref.month < 7:
        return datetime.date(ref.year, 3, 1)
    elif ref.month < 10:
        return datetime.date(ref.year, 6, 1)
    return datetime.date(ref.year, 9, 1)


def question_clear(question_ask):
    """
    Clearing question for duration
    """
    question = question_ask

    dic = {"on time install", "oti", "on-time install", '% on-time install',
           '% on time install', "% install", "on time delivery",
           "otd", "on-time delivery", '% on-time delivery',
           '% on-time delivery', '% delivery',
           "fmi", "recall total cost", 'recalls', 'recall', 'number of recalls',
           "complaint rate", "complaint/ib", "complaints/ib",
           'complaints/install base', 'complaints',
           "obf", "out of box failure", "out of box", "out of box failures",
           "elf", "early life failure”, “early life failures", "elf 90", "elf90",
           "ifr", "intial failure rate", "ifr 90", "ifr90",
           "warranty per unit", "wpu",
           "gsc scrap", "scrap", "gsc",
           "cso", "open red cso", "open red", "# open red > 90 days",
           "red cso", "red csos",
           "cso total", "total cso", "open cso", "# of total csos", "# of total cso",
           "cost to serve", "cts", "cost to serve on contract",
           "foi", 'failure on install', 'install failure rate'
           }

    question = replace_all(question, dic)
    question = question.replace('for', "")
    question = question.replace('available', "")
    question = question.replace("is", "")
    question = question.replace("show", "")
    question = question.replace("trend", "")

    question = question.upper()
    return question


def result_check(result):
    """
    Passing the correct result
    """
    if result == '':
        result = 1
        return result
    else:
        return int(result)


def curre_month(current_month, today):
    """
    Passing the correct month and correct year
    """
    if current_month == 0:
        return 12, today.year - 1
    else:
        return current_month, today.year


def week_checker(to_week_number, to_year):
    """
    Passing the correct week and year
    """
    if to_week_number == 0:
        to_week_number = 52,
        to_year = to_year - 1
    return to_week_number, to_year


def year_checker(year_obtained, today):
    """
    Passing the correct correct year
    """
    if len(year_obtained) == 0:
        to_year = today.isocalendar()[0]
        from_year = today.isocalendar()[0]
    else:
        to_year = year_obtained[0]
        from_year = year_obtained[0]
    return to_year, from_year


def rolling_we(question):
    """
    Identifying number of weeks
    """
    if "ROLLING" in question:
        return re.search('ROLLING(.*)WEEK', question).group(1)
    else:
        return re.search('LAST(.*)WEEK', question).group(1)


def rolling_quarter(question):
    """
    Identifying number of quarters
    """
    if "ROLLING" in question:
        return re.search('ROLLING(.*)QUARTER', question).group(1)
    else:
        return re.search('LAST(.*)QUARTER', question).group(1)


def ytd_checker(ytd_check, year_check, today):
    """
    Identifying to and from year
    """
    print("Check", len(ytd_check))
    if len(ytd_check) == 0:
        print("One")
        return today.isocalendar()[0], today.isocalendar()[0]
    elif len(ytd_check) >= 1:
        print("here")
        year_obtained = [i for i in ytd_check if i in year_check]
        print(year_obtained)

        to_year, from_year = year_checker(year_obtained, today)
    return to_year, from_year


def rolling_one_quarter(from_duration_o, to_duration_o, result):
    if result == 1:
        from_duration_o = to_duration_o
    return from_duration_o, to_duration_o


def time_check(time_got, week_ind, question):
    """
    Get Time tagger
    :param time_got: time received by dialog flow
    :param week_ind: week indicator
    :param question: text received from user
    :return:
    """
    today = datetime.date.today()
    if "/" not in time_got:
        if (("ROLLING" or "LAST") and ("QUARTER" or "QUARTERS")) in question:
            print(question)
            print("last quarter loop")
            from_duration = ""

            question = question.replace(" ", "")
            question = question.replace("QUARTERS", "QUARTER")

            to_duration_o = previous_quarter(today)
            result = rolling_quarter(question)

            result = result_check(result)  # .astype(int)
            from_duration_o = today - relativedelta(months=result * 3)
            x_org = pd.to_datetime(from_duration_o)
            x_qtr = datetime.date(x_org.year, 3 * ((x_org.month - 1) // 3) + 1, 1)
            from_duration_o = x_qtr
            from_duration_o = from_duration_o.strftime('%Y-%m-%d')
            to_duration_o = to_duration_o.strftime('%Y-%m-%d')

            from_duration, to_duration = rolling_one_quarter(from_duration_o, to_duration_o, result)

            return from_duration + "/" + to_duration


        elif ("ROLLING" and ("MONTH" or "MONTHS")) in question:
            print("rolling month loop")
            question = question.replace(" ", "")
            question = question.replace("MONTHS", "MONTH")
            result = re.search(('ROLLING(.*)MONTH'), question).group(1)

            result = result_check(result)  # .astype(int)

            current_month = today.month - 1
            current_month, current_year = curre_month(current_month, today)

            from_day = date.today() - relativedelta(months=result)
            from_year = from_day.year
            from_month = from_day.month

            return str(from_year) + "-" + str(from_month) + "-" + "01" + \
                   "/" + str(current_year) + "-" + str(current_month) + "-" + "01"

        elif week_ind:
            time_got = week_work(question)
            return time_got


    else:
        return time_got


def week_work(question):
    """
    Identifying week number and year
    """
    if ("ROLLING" or "LAST") and ("WEEK" or "WEEKS") in question:
        print("last week loop")
        question = question.replace(" ", "")
        question = question.replace("WEEKS", "WEEK")

        result = rolling_we(question)

        result = result_check(result)

        today = datetime.date.today()
        to_week_number_x = today.isocalendar()[1] - 1
        to_year_x = today.isocalendar()[0]

        to_week_number, to_year = week_checker(to_week_number_x, to_year_x)

        weekday = today.weekday()
        start_delta = datetime.timedelta(days=weekday, weeks=result)
        start_of_week = today - start_delta

        from_week_number = start_of_week.isocalendar()[1]
        from_year = start_of_week.isocalendar()[0]
        print(str(from_year) + "-" + str(from_week_number) + "/" + str(to_year) + "-" + str(
            to_week_number))
        return str(from_year) + "-" + str(from_week_number) + "/" + str(to_year) + "-" + str(
            to_week_number)

    if "YTD" in question:
        print("YTD loop")
        today = datetime.date.today()
        to_week_number = today.isocalendar()[1] - 1
        ytd_check = [int(s) for s in question.split() if s.isdigit()]
        print(ytd_check)
        year_check = [2017, 2018, 2019]
        to_year, from_year = ytd_checker(ytd_check, year_check, today)

        return str(from_year) + "-" + str(1) + "/" + str(to_year) + "-" + str(
            to_week_number)


def duration_finder(time, week, question_ask):
    """
    Duration Finder
    """
    question = question_clear(question_ask)
    time_got = time_check(time, week, question)

    return time_got


def init_func(list_a):
    """
    Intialization
    """
    print("Main loop")
    print(list_a)
    print(list_a['metric'])
    metric = list_a['metric']
    duration = duration_finder(list_a['time'], list_a['week'], list_a['text'])
    item = item_finder(list_a['text'])
    week_ind = list_a['week']
    print({"metric": metric, "item": item, 'time': duration, 'week': week_ind})
    return {"metric": metric, "item": item, 'time': duration, 'week': week_ind}

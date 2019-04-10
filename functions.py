import datetime
import calendar
import numpy as np
import pandas as pd
import scorecard_functions

current_year = datetime.datetime.now().year
previous_year = current_year - 1
current_month = datetime.datetime.now().month


def setup_data_for_cr_calculations(data):
    group_number_of_complaints = \
        data.loc[data['Measure Names'] == '# of Complaints'] \
            .groupby(
            ['SC Business Segment', 'SC Modality', 'SC Product Segment',
             'SC Product', 'By Time Label', 'Year', 'Month', "Week Number",
             'Measure Names'])['Measure Values'].sum()
    group_ib = data.loc[data['Measure Names'] == 'Average IB Over Time'] \
        .groupby(
        ['SC Business Segment', 'SC Modality', 'SC Product Segment',
         'SC Product', 'By Time Label', 'Year', 'Month', "Week Number",
         'Measure Names'])['Measure Values'].sum()

    final_data = pd.DataFrame(group_number_of_complaints).reset_index()
    final_data = final_data.append(pd.DataFrame(group_ib).reset_index())
    return final_data


def setup_data_for_cr_calculations_chu(data):
    group_number_of_complaints = \
        data.loc[data['Measure Names'] == '# of Complaints'] \
            .groupby(
            ['SC Business Segment', 'SC Modality', 'SC Product Segment',
             'SC Product', 'Year', 'Quarter',
             'Measure Names'])['Measure Values'].sum()
    group_ib = data.loc[data['Measure Names'] == 'Average IB Over Time'] \
        .groupby(
        ['SC Business Segment', 'SC Modality', 'SC Product Segment',
         'SC Product', 'Year', 'Quarter',
         'Measure Names'])['Measure Values'].sum()

    final_data = pd.DataFrame(group_number_of_complaints).reset_index()
    final_data = final_data.append(pd.DataFrame(group_ib).reset_index())
    return final_data


def setup_scorecard_structure(data, level):
    """
    Setup scorecard structure with columns "Product Quality Scorecard" and
    computation columns from 2016 onwards
    :param data: source data after hierarchy lookup (data frame)
    :param level: modality (or) product (string)
    :return: scorecard structure (data frame)
    """

    score_card = scorecard_functions.setup_scorecard_columns()
    index = 0
    score_card.loc[index, 'Product Quality Scorecard'] = 'GEHC'
    if level != 'modality':
        data['SC Product'].fillna("", inplace=True)
    for business in data['SC Business Segment'].sort_values(
            ascending=False).unique():
        index = index + 1
        score_card.loc[index, 'Product Quality Scorecard'] = business
        for modality in np.unique(
                data.loc[data['SC Business Segment'] == business][
                    'SC Modality'].values):
            index = index + 1
            score_card.loc[index, 'Product Quality Scorecard'] = modality
            score_card.loc[index, 'Business'] = business
            if level == 'product':
                index, score_card = scorecard_functions.setup_scorecard_product_segments(data,
                                                                                         score_card,
                                                                                         modality,
                                                                                         business,
                                                                                         index)
            if level == "chu-product":
                products_list = np.unique(data.loc[data['SC Modality'] == str(modality)] \
                                              ['SC Product'].values)
                index, score_card = scorecard_functions.setup_scorecard_products(products_list,
                                                                                 index, score_card,
                                                                                 modality, business,
                                                                                 None)
    if level == 'product':
        index = index + 1
        score_card.loc[index, 'Product Quality Scorecard'] = 'LCS'
        score_card.loc[index, 'Business'] = 'CCS'
    if level == "chu-product":
        index = index + 1
        score_card.loc[index, 'Product Quality Scorecard'] = "WOMEN'S HEALTH"
        score_card.loc[index, "Business"] = "IMAGING"
        index = index + 1
        score_card.loc[index, 'Product Quality Scorecard'] = "MICT"
        score_card.loc[index, "Business"] = "IMAGING"

    score_card['Business'].fillna('ALL', inplace=True)
    score_card['Modality'].fillna('ALL', inplace=True)
    if level == "modality" or level == "product":
        score_card['Product Segment'].fillna('ALL', inplace=True)

    return score_card


def setup_sii_sc_hierarchy(data, sii_hierarchy, modality_lookup, metric):
    """
    Setup SII Hierarchy on source data
    :param data: data from source (data frame)
    :param sii_hierarchy: SII Hierarchy lookup (data frame)
    :param modality_lookup: Additional metric specific lookup (data frame)
    :param metric: name of the metric (string)
    :return: data with sii hierarchy (data frame)
    """
    today = datetime.date.today()
    weekday = today.weekday()
    start_delta = datetime.timedelta(days=weekday, weeks=1)
    start_of_week = today - start_delta

    modality_metric_column = ''
    sub_modality_metric_column = ''
    config_modality_column = ''
    a_level_modality_column = ''
    if metric == 'OTD':
        modality_metric_column = 'SII Modality + DV Modality'
        config_modality_column = 'DV Modality'
        sub_modality_metric_column = 'SIISUBMODALITY'
        a_level_modality_column = 'a_level_modality_code'
        abbr_to_num = {name: num for num, name in
                       enumerate(calendar.month_name) if num}
        months = data['Month Name'].unique()
        for month in months:
            data.loc[data['Month Name'] == month, 'Month'] = abbr_to_num[month]
        data['Year'] = data['Year'].astype(int)
        data['Month'] = data['Month'].astype(int)
        if data['Number of Records'].dtype == np.object:
            data['Number of Records'] = data['Number of Records'].str.replace(',', '').astype(int)
        else:
            data['Number of Records'] = data['Number of Records'].astype(int)

    elif metric == 'OTI':
        modality_metric_column = 'SII Modality or OTI Modality'
        sub_modality_metric_column = 'SUB_MODLTY_CD'
        a_level_modality_column = 'a_level_modality_code'
        config_modality_column = 'Modality (SFDC)'
        time_labels = data['By Time Label'].unique()
        for time_label in time_labels:
            data.loc[data['By Time Label'] == time_label, "Year"] = \
                time_label.split('M')[0]
            data.loc[data['By Time Label'] == time_label, "Month"] = \
                time_label.split('M')[1]
        data['Year'] = data['Year'].astype(int)
        data['Month'] = data['Month'].astype(int)
    elif metric == 'CSO':
        modality_metric_column = 'SII Modality or CSO Modality'
        a_level_modality_column = 'a_level_modality'
        sub_modality_metric_column = 'SII_sub_Modality'
        config_modality_column = 'Modality Group (SFDC)'
        time_labels = data['Snapshot FW'].unique()
        for time_label in time_labels:
            data.loc[data['Snapshot FW'] == time_label, "Year"] = 2000 + int(
                time_label.split('-')[0])
            data.loc[data['Snapshot FW'] == time_label, 'Week'] = int(
                time_label.split('-')[1])
        data['Year'] = data['Year'].astype(int)
        data['Week'] = data['Week'].astype(int)
    elif metric == 'CSO Total':
        modality_metric_column = 'SII Modality or CSO Modality'
        a_level_modality_column = 'a_level_modality'
        sub_modality_metric_column = 'SII_sub_Modality'
        config_modality_column = 'Modality Group (SFDC)'
        time_labels = data['Snapshot FW'].unique()
        for time_label in time_labels:
            data.loc[data['Snapshot FW'] == time_label, "Year"] = 2000 + int(
                time_label.split('-')[0])
            data.loc[data['Snapshot FW'] == time_label, 'Week'] = int(
                time_label.split('-')[1])

    elif metric == 'ELF':
        modality_metric_column = 'modality_code'
        a_level_modality_column = 'a_level_modality_code'
        sub_modality_metric_column = 'business_segment_code'
        config_modality_column = 'Adjusted Modality Group'
        time_labels = data['Time Label'].unique()
        for time_label in time_labels:
            data.loc[data[
                         'Time Label'] == time_label,
                     "Year of service_close_date"] = \
                time_label.split('M')[0]
            data.loc[data['Time Label'] == time_label, 'Month Value'] = \
                time_label.split('M')[1]
        data['Year of service_close_date'] = data[
            'Year of service_close_date'].astype(int)
        data['Month Value'] = data['Month Value'].astype(int)
        if data['TOTALGECOST'].dtype == np.object:
            data['TOTALGECOST'] = data['TOTALGECOST'].str.replace(',', '') \
                .astype(float)
        else:
            data['TOTALGECOST'] = data['TOTALGECOST'].astype(float)
    elif metric == 'FMI':
        modality_metric_column = 'SII Modality Code'
        a_level_modality_column = 'A_Level_Modality'
        sub_modality_metric_column = 'SII Sub Modality'
        config_modality_column = 'SII Modality Group'
        time_labels = data['Time Axis'].unique()
        for time_label in time_labels:
            data.loc[data['Time Axis'] == time_label, "Year"] = \
                time_label.split('-')[0]
            data.loc[data['Time Axis'] == time_label, 'Month'] = \
                time_label.split('-')[1]
        data['Year'] = data['Year'].astype(int)
        data['Month'] = data['Month'].astype(int)
        if data['Total Cost'].dtype == np.object:
            data['Total Cost'] = data['Total Cost'].str.replace(",", "").astype(
                float)
        else:
            data['Total Cost'] = data['Total Cost'].astype(
                float)
        data.rename(
            columns={'Product Group / Sub Family': 'Scorecard Product'},
            inplace=True)
    elif metric == 'WPU_SPEND':
        modality_metric_column = 'SII Modality'
        a_level_modality_column = 'a_level_modality'
        sub_modality_metric_column = 'SII sub modality'
        data.rename(
            columns={'Product Group or Sub Family': 'Scorecard Product'},
            inplace=True)
        time_labels = data['Time Label'].unique()
        for time_label in time_labels:
            data.loc[data['Time Label'] == time_label, "Year"] = \
                str(time_label)[:4]
            data.loc[data['Time Label'] == time_label, 'Month'] = \
                str(time_label)[-2:]
        data['Year'] = data['Year'].astype(int)
        data['Month'] = data['Month'].astype(int)
        if data['Total GE Cost'].dtype == np.object:
            data['Total GE Cost'] = data['Total GE Cost'].str.replace(',', '') \
                .astype(float)

    elif metric == 'WPU_IB' or metric == 'CTS_IB':
        modality_metric_column = 'SII_Modality'
        a_level_modality_column = 'A_Level_Modality'
        sub_modality_metric_column = 'SII_Sub_Modality'
        data.rename(
            columns={'Product Group / Sub Family': 'Scorecard Product'},
            inplace=True)
        time_labels = data['snapshot_name'].unique()
        for time_label in time_labels:
            data.loc[data['snapshot_name'] == time_label, "Year"] = str(
                time_label)[:4]
            data.loc[data['snapshot_name'] == time_label, 'Month'] = str(
                time_label)[-2:]
        data['Year'] = data['Year'].astype(int)
        data['Month'] = data['Month'].astype(int)
        if data['sum'].dtype == np.object:
            data['sum'] = data['sum'].str.replace(',', '').astype(int)
        else:
            data['sum'] = data['sum'].astype(int)

    elif metric == 'CTS_SPEND':
        modality_metric_column = 'SII Modality'
        a_level_modality_column = 'A_Level_Modality_Code'
        sub_modality_metric_column = 'SII Sub modality'
        data.rename(
            columns={'Product Group / SII Sub Family': 'Scorecard Product'},
            inplace=True)
        time_labels = data['Time Label'].unique()
        for time_label in time_labels:
            data.loc[data['Time Label'] == time_label, "Year"] = \
                str(time_label)[0:4]
            data.loc[data['Time Label'] == time_label, 'Month'] = \
                str(time_label)[-2:]
        data['Year'] = data['Year'].astype(int)
        data['Month'] = data['Month'].astype(int)
        if data['Total GE Cost'].dtype == np.object:
            data['Total GE Cost'] = data['Total GE Cost'].str.replace(',', '') \
                .astype(float)

    data[modality_metric_column].fillna('', inplace=True)
    data[sub_modality_metric_column].fillna('', inplace=True)
    data['Scorecard Product'].fillna('', inplace=True)
    data[a_level_modality_column].fillna('', inplace=True)
    data['lookup'] = data[modality_metric_column] + data[
        sub_modality_metric_column] + data['Scorecard Product']
    data['SC Product'] = ''
    if "WPU" not in metric and "CTS" not in metric:
        lookup_for_null_modality = \
            data.loc[data[a_level_modality_column] == '']['lookup'].unique()
        for lookup_value in lookup_for_null_modality:
            lookup_data = modality_lookup.loc[
                modality_lookup[config_modality_column] == lookup_value]
            if lookup_data.size > 0:
                data.loc[
                    data['lookup'] == lookup_value, 'SC Business Segment'] = \
                    lookup_data['Business'].values[
                        0].strip()
                data.loc[data['lookup'] == lookup_value, 'SC Modality'] = \
                    lookup_data['Modality'].values[0].strip()
                data.loc[data[
                             'lookup'] == lookup_value, 'SC Product Segment'] = 'OTHER'
            else:
                data.loc[data[
                             'lookup'] == lookup_value, 'SC Business Segment'] = 'GEHC OTHER'
                data.loc[
                    data['lookup'] == lookup_value, 'SC Modality'] = 'OTHER'
                data.loc[data[
                             'lookup'] == lookup_value, 'SC Product Segment'] = 'OTHER'
    # update scorecard mapping for records that have value in a_level_modality_code
    lookup_for_not_null_modality = \
        data.loc[data[a_level_modality_column] != '']['lookup'].unique()
    for lookup_value in lookup_for_not_null_modality:

        sii_data = sii_hierarchy.loc[
            sii_hierarchy['lookup value'] == lookup_value]
        if sii_data.size > 0:
            data.loc[data['lookup'] == lookup_value, 'SC Business Segment'] = \
                sii_data['Business'].values[0]
            data.loc[data['lookup'] == lookup_value, 'SC Modality'] = \
                sii_data['Modality'].values[0]
            data.loc[data['lookup'] == lookup_value, 'SC Product Segment'] = \
                sii_data['Product Segment'].values[0]
            data.loc[data['lookup'] == lookup_value, 'SC Product'] = \
                sii_data['sub_family_code (Ultrasound Only) ' \
                         'or product_group_code'].values[0]
        else:

            a_level_modality_data = data.loc[data['lookup'] == lookup_value][
                a_level_modality_column]
            if a_level_modality_data.size > 0:
                a_level_modality = a_level_modality_data.unique()

                for modality in a_level_modality:
                    sub_modality_list = \
                        data.loc[(data['lookup'] == lookup_value) & (
                                data[a_level_modality_column] == modality)][
                            sub_modality_metric_column].unique()
                    for sub_modality in sub_modality_list:
                        sii_data = sii_hierarchy.loc[
                            (sii_hierarchy[
                                 'a_level_modality_code'] == modality) & (
                                    sii_hierarchy[
                                        'business_segment_code'] == sub_modality)
                            ]

                        if sii_data.size > 0:
                            data.loc[(data['lookup'] == lookup_value) & (
                                    data[a_level_modality_column] == modality) & (
                                             data[
                                                 sub_modality_metric_column] == sub_modality),
                                     'SC Business Segment'] = \
                                sii_data['Business'].values[0].strip()
                            data.loc[(data['lookup'] == lookup_value) & (
                                    data[a_level_modality_column] == modality) & (
                                             data[
                                                 sub_modality_metric_column] == sub_modality),
                                     'SC Modality'] = \
                                sii_data['Modality'].values[
                                    0].strip()
                            data.loc[(data['lookup'] == lookup_value) & (
                                    data[a_level_modality_column] == modality),
                                     'SC Product Segment'] = 'OTHER'

                        else:
                            data.loc[(data['lookup'] == lookup_value) & (
                                    data[a_level_modality_column] == modality),
                                     'SC Business Segment'] = 'GEHC OTHER'
                            data.loc[(data['lookup'] == lookup_value) & (
                                    data[a_level_modality_column] == modality),
                                     'SC Modality'] = 'OTHER'
                            data.loc[(data['lookup'] == lookup_value) & (
                                    data[a_level_modality_column] == modality),
                                     'SC Product Segment'] = 'OTHER'
            else:
                data.loc[data[
                             'lookup'] == lookup_value, 'SC Business Segment'] = 'GEHC OTHER'
                data.loc[
                    data['lookup'] == lookup_value, 'SC Modality'] = 'OTHER'
                data.loc[data[
                             'lookup'] == lookup_value, 'SC Product Segment'] = 'OTHER'
    # set scorecard product level data if it is not set yet
    data['SC Product'].fillna('', inplace=True)
    blank_scorecard_products = data.loc[data['SC Product'] == '']
    if blank_scorecard_products.size > 0:
        products = data.loc[data['SC Product'] == ''][
            'Scorecard Product'].unique()
        for product in products:
            scorecard_product = ''
            if product == '' or product == 'NAN' or product == 'nan' or \
                    product is None:
                scorecard_product = '<BLANK IN SOURCE>'
            else:
                scorecard_product = product

            data.loc[(data['SC Product'] == '') & (
                    data['Scorecard Product'] == product), 'SC Product'] = \
                scorecard_product
    data['SC Business Segment'].fillna('', inplace=True)
    data['SC Modality'].fillna('', inplace=True)
    data['SC Product Segment'].fillna('', inplace=True)
    data.loc[data['SC Business Segment'] == '', 'SC Business Segment'] = \
        'GEHC OTHER'
    data.loc[data['SC Modality'] == '', 'SC Modality'] = 'OTHER'
    data.loc[data['SC Product Segment'] == '', 'SC Product Segment'] = 'OTHER'

    today = datetime.date.today()
    weekday = today.weekday()
    start_delta = datetime.timedelta(days=weekday, weeks=1)
    start_of_week = today - start_delta
    week_number = start_of_week.isocalendar()[1]

    current_year = datetime.datetime.now().year
    current_month = datetime.datetime.now().month
    if metric == 'CTS_SPEND' or metric == 'WPU_SPEND' or metric == "WPU_IB" or metric == "CTS_IB":
        data = data.loc[(data['Year'] < current_year) |
                        ((data['Year'] == current_year) &
                         (data['Month'] <= (current_month)))]

    elif metric == 'ELF':
        data = data.loc[((data['Year of service_close_date'] < current_year)
                         & (data['Year of service_close_date'] >= 2016)) |
                        ((data['Year of service_close_date'] == current_year) &
                         (data['Month Value'] <= (current_month)) & (
                                 data['Week Number'] <= (week_number)))]

    elif metric != 'CSO' and metric != 'ELF' and metric != 'CSO Total':
        data = data.loc[(data['Year'] < current_year) |
                        ((data['Year'] == current_year) &
                         (data['Month'] <= (current_month)) & (
                                 data['Week Number'] <= (week_number)))]
    else:
        data = data.loc[(data['Year'] < current_year) |
                        ((data['Year'] == current_year) &
                         (data['Week'] <= week_number))]

    return data


def calculate_otd_q4_rolling13(score_card, data, metric):
    """
    Calculate On Time Delivery for scorecard
    :param score_card: scorecard structure for the metric (data frame)
    :param data: source data after hierarchy look up (data frame)
    :return: scorecard data
    """
    for index, row in score_card.iterrows():

        if scorecard_functions.is_gehc_level(row):
            gehc_on_time = data.loc[data['DV On Time?'] == 'On Time'].groupby('Year')[
                'Number of Records'].sum()
            gehc_overall = data.groupby('Year')['Number of Records'].sum()
            gehc_otd = gehc_on_time / gehc_overall * 100
            score_card = scorecard_functions.set_metric_value_current(gehc_otd, index, score_card)

            gehc_13_week = scorecard_functions.get_n_rolling_week_data(data, 13)

            gehc_on_time_13 = \
                gehc_13_week.loc[gehc_13_week['DV On Time?'] == 'On Time'][
                    'Number of Records'].sum()
            gehc_overall_13 = gehc_13_week['Number of Records'].sum()

            gehc_otd_13 = gehc_on_time_13 / gehc_overall_13 * 100
            score_card = scorecard_functions.set_sc_column_value(gehc_otd_13, index,
                                                                 "Rolling 13 weeks", score_card)

            gehc_previous_ytd = scorecard_functions.get_previous_year_data(data, metric)
            gehc_on_time_ytd = \
                gehc_previous_ytd.loc[gehc_previous_ytd['DV On Time?'] == 'On Time'].groupby(
                    'Year')[
                    'Number of Records'].sum()
            gehc_overall_ytd = gehc_previous_ytd.groupby('Year')['Number of Records'].sum()
            gehc_otd_ytd = gehc_on_time_ytd / gehc_overall_ytd * 100

            score_card = scorecard_functions.set_metric_value_previous(gehc_otd_ytd, index,
                                                                       score_card)
        elif scorecard_functions.is_business_level(row):
            business = scorecard_functions.get_business_data(data, row, False, 0, metric)

            business_on_time = business.loc[business['DV On Time?'] == 'On Time'].groupby('Year') \
                ['Number of Records'].sum()
            business_overall = business.groupby('Year')['Number of Records'].sum()
            business_otd = business_on_time / business_overall * 100
            score_card = scorecard_functions.set_metric_value_current(business_otd, index,
                                                                      score_card)
            business_13_week = scorecard_functions.get_business_data(data, row, False, 13, metric)
            business_on_time_13 = \
                business_13_week.loc[business_13_week['DV On Time?'] == 'On Time'][
                    'Number of Records'].sum()
            business_overall_13 = business_13_week['Number of Records'].sum()
            business_otd_13 = business_on_time_13 / business_overall_13 * 100
            score_card = scorecard_functions.set_sc_column_value(business_otd_13, index,
                                                                 "Rolling 13 weeks", score_card)

            business_previous_ytd = scorecard_functions.get_business_data(data, row, True, 0,
                                                                          metric)

            business_on_time_ytd = \
                business_previous_ytd.loc[
                    business_previous_ytd['DV On Time?'] == 'On Time'].groupby(
                    'Year')['Number of Records'].sum()

            business_overall_ytd = business_previous_ytd.groupby('Year')['Number of Records'].sum()

            business_otd_ytd = business_on_time_ytd / business_overall_ytd * 100

            score_card = scorecard_functions.set_metric_value_previous(business_otd_ytd, index,
                                                                       score_card)

        elif scorecard_functions.is_modality_level(row=row, is_lcs=False):
            modality = scorecard_functions.get_modality_data(data=data, row=row, is_lcs=False,
                                                             is_previous_year=False,
                                                             number_of_weeks=0, metric=metric)

            modality_on_time = modality.loc[modality['DV On Time?'] == 'On Time'].groupby('Year') \
                ['Number of Records'].sum()

            modality_overall = modality.groupby('Year')['Number of Records'].sum()

            modality_otd = modality_on_time / modality_overall * 100
            score_card = scorecard_functions.set_metric_value_current(modality_otd, index,
                                                                      score_card)
            modality_13_week = scorecard_functions.get_modality_data(data=data, row=row,
                                                                     is_lcs=False,
                                                                     is_previous_year=False,
                                                                     number_of_weeks=13,
                                                                     metric=metric)
            modality_on_time_13 = \
                modality_13_week.loc[modality_13_week['DV On Time?'] == 'On Time'][
                    'Number of Records'].sum()

            modality_overall_13 = modality_13_week['Number of Records'].sum()

            modality_otd_13 = modality_on_time_13 / modality_overall_13 * 100
            score_card = scorecard_functions.set_sc_column_value(modality_otd_13, index,
                                                                 "Rolling 13 weeks", score_card)

            modality_previous_ytd = scorecard_functions.get_modality_data(data=data, row=row,
                                                                          is_lcs=False,
                                                                          is_previous_year=True,
                                                                          number_of_weeks=0,
                                                                          metric=metric)

            modality_on_time_ytd = \
                modality_previous_ytd.loc[
                    modality_previous_ytd['DV On Time?'] == 'On Time'].groupby(
                    'Year')['Number of Records'].sum()

            modality_overall_ytd = modality_previous_ytd.groupby('Year')['Number of Records'].sum()

            modality_otd_ytd = modality_on_time_ytd / modality_overall_ytd * 100

            score_card = scorecard_functions.set_metric_value_previous(modality_otd_ytd, index,
                                                                       score_card)
        elif scorecard_functions.is_modality_level(row=row, is_lcs=True):

            modality = scorecard_functions.get_modality_data(data=data, row=row, is_lcs=True,
                                                             is_previous_year=False,
                                                             number_of_weeks=0, metric=metric)

            modality_on_time = modality.loc[modality['DV On Time?'] == 'On Time'].groupby('Year') \
                ['Number of Records'].sum()

            modality_overall = modality.groupby('Year')['Number of Records'].sum()

            modality_otd = modality_on_time / modality_overall * 100
            score_card = scorecard_functions.set_metric_value_current(modality_otd, index,
                                                                      score_card)

            modality_13_week = scorecard_functions.get_modality_data(data=data, row=row,
                                                                     is_lcs=True,
                                                                     is_previous_year=False,
                                                                     number_of_weeks=13,
                                                                     metric=metric)
            modality_on_time_13 = \
                modality_13_week.loc[modality_13_week['DV On Time?'] == 'On Time'][
                    'Number of Records'].sum()

            modality_overall_13 = modality_13_week['Number of Records'].sum()

            modality_otd_13 = modality_on_time_13 / modality_overall_13 * 100
            score_card = scorecard_functions.set_sc_column_value(modality_otd_13, index,
                                                                 "Rolling 13 weeks", score_card)

            modality_previous_ytd = scorecard_functions.get_modality_data(data=data, row=row,
                                                                          is_lcs=True,
                                                                          is_previous_year=True,
                                                                          number_of_weeks=0,
                                                                          metric=metric)

            modality_on_time_ytd = \
                modality_previous_ytd.loc[
                    modality_previous_ytd['DV On Time?'] == 'On Time'].groupby(
                    'Year')['Number of Records'].sum()

            modality_overall_ytd = modality_previous_ytd.groupby('Year')['Number of Records'].sum()

            modality_otd_ytd = modality_on_time_ytd / modality_overall_ytd * 100

            score_card = scorecard_functions.set_metric_value_previous(modality_otd_ytd, index,
                                                                       score_card)

        elif scorecard_functions.is_product_segment_level(row):
            prod_seg = scorecard_functions.get_product_segment_data(data, row, False, 0, metric)

            prod_seg_on_time = prod_seg.loc[prod_seg['DV On Time?'] == 'On Time'].groupby('Year')[
                'Number of Records'].sum()

            prod_seg_overall = prod_seg.groupby('Year')['Number of Records'].sum()

            prod_seg_otd = prod_seg_on_time / prod_seg_overall * 100

            score_card = scorecard_functions.set_metric_value_current(prod_seg_otd, index,
                                                                      score_card)
            prod_seg_13_week = scorecard_functions.get_product_segment_data(data, row, False, 13,
                                                                            metric)

            prod_seg_on_time_13 = \
                prod_seg_13_week.loc[prod_seg_13_week['DV On Time?'] == 'On Time'][
                    'Number of Records'].sum()

            prod_seg_overall_13 = prod_seg_13_week['Number of Records'].sum()

            prod_seg_otd_13 = prod_seg_on_time_13 / prod_seg_overall_13 * 100

            score_card = scorecard_functions.set_sc_column_value(prod_seg_otd_13, index,
                                                                 "Rolling 13 weeks", score_card)
            prod_seg_ytd_previous = scorecard_functions.get_product_segment_data(data, row, True, 0,
                                                                                 metric)
            prod_seg_on_time_ytd = \
                prod_seg_ytd_previous.loc[
                    prod_seg_ytd_previous['DV On Time?'] == 'On Time'].groupby('Year')[
                    'Number of Records'].sum()

            prod_seg_overall_ytd = prod_seg_ytd_previous.groupby('Year')['Number of Records'].sum()

            prod_seg_otd = prod_seg_on_time_ytd / prod_seg_overall_ytd * 100

            if len(prod_seg_otd.index.get_level_values(0)) > 0:
                score_card = scorecard_functions.set_metric_value_previous(prod_seg_otd, index,
                                                                           score_card)

        else:
            product = scorecard_functions.get_product_data(data, row, False, 0, metric)

            product_on_time = \
                product.loc[
                    product['DV On Time?'] == 'On Time'].groupby('Year')[
                    'Number of Records'].sum()

            product_overall = product.groupby('Year')['Number of Records'].sum()

            product_otd = product_on_time / product_overall * 100

            score_card = scorecard_functions.set_metric_value_current(product_otd, index,
                                                                      score_card)

            product_13_week = scorecard_functions.get_product_data(data, row, False, 13, metric)

            product_on_time_13 = \
                product_13_week.loc[product_13_week['DV On Time?'] == 'On Time'][
                    'Number of Records'].sum()

            product_overall_13 = product_13_week['Number of Records'].sum()

            product_otd_13 = product_on_time_13 / product_overall_13 * 100

            score_card = scorecard_functions.set_sc_column_value(product_otd_13, index,
                                                                 "Rolling 13 weeks", score_card)

            product_previous_ytd = scorecard_functions.get_product_data(data, row, True, 0, metric)

            product_on_time_ytd = \
                product_previous_ytd.loc[product_previous_ytd['DV On Time?'] == 'On Time'].groupby(
                    'Year')[
                    'Number of Records'].sum()

            product_overall_ytd = product_previous_ytd.groupby('Year')['Number of Records'].sum()

            product_otd_ytd = product_on_time_ytd / product_overall_ytd * 100
            if len(product_otd_ytd.index.get_level_values(0)) > 0:
                score_card = scorecard_functions.set_metric_value_previous(product_otd_ytd, index,
                                                                           score_card)
    return score_card


def calculate_oti_q4_rolling13(score_card, data, metric):
    """
        Calculate On Time Install for scorecard with Q4 value for the previous
        year
        :param score_card: scorecard structure for the metric (data frame)
        :param data: source data after hierarchy look up (data frame)
        :return: scorecard data
        """
    for index, row in score_card.iterrows():
        if scorecard_functions.is_gehc_level(row):
            gehc_on_time = data.loc[
                data['Measure Names'] == '# of On Time Install'].groupby(
                'Year')['Measure Values'].sum()
            gehc_overall = data.groupby('Year')['Measure Values'].sum()
            gehc_oti = gehc_on_time / gehc_overall * 100
            score_card = scorecard_functions.set_metric_value_current(gehc_oti, index, score_card)

            gehc_13_week = scorecard_functions.get_n_rolling_week_data(data, 13)
            gehc_on_time_13 = \
                gehc_13_week.loc[gehc_13_week['Measure Names'] == '# of On Time Install'][
                    'Measure Values'].sum()
            gehc_overall_13 = gehc_13_week['Measure Values'].sum()
            gehc_oti_13 = gehc_on_time_13 / gehc_overall_13 * 100

            score_card = scorecard_functions.set_sc_column_value(gehc_oti_13, index,
                                                                 "Rolling 13 weeks", score_card)

            gehc_previous_ytd = scorecard_functions.get_previous_year_data(data, metric)

            gehc_on_time_ytd = gehc_previous_ytd.loc[
                gehc_previous_ytd['Measure Names'] == '# of On Time Install'].groupby('Year')[
                'Measure Values'].sum()
            gehc_overall_ytd = gehc_previous_ytd.groupby('Year')['Measure Values'].sum()
            gehc_oti_ytd = gehc_on_time_ytd / gehc_overall_ytd * 100
            score_card = scorecard_functions.set_metric_value_previous(gehc_oti_ytd, index,
                                                                       score_card)
        elif scorecard_functions.is_business_level(row):
            business = scorecard_functions.get_business_data(data, row, False, 0, metric)

            business_on_time = \
                business.loc[business['Measure Names'] == '# of On Time Install'].groupby('Year')[
                    'Measure Values'].sum()
            business_overall = business.groupby('Year')['Measure Values'].sum()
            business_oti = business_on_time / business_overall * 100
            score_card = scorecard_functions.set_metric_value_current(business_oti, index,
                                                                      score_card)
            business_13_week = scorecard_functions.get_business_data(data, row, False, 13, metric)

            business_on_time_13 = business_13_week.loc[
                business_13_week['Measure Names'] == '# of On Time Install'][
                'Measure Values'].sum()
            business_overall_13 = business_13_week['Measure Values'].sum()
            business_oti_13 = business_on_time_13 / business_overall_13 * 100
            score_card = scorecard_functions.set_sc_column_value(business_oti_13, index,
                                                                 "Rolling 13 weeks", score_card)

            business_previous_ytd = scorecard_functions.get_business_data(data, row, True, 0,
                                                                          metric)

            business_on_time_ytd = business_previous_ytd.loc[
                (business_previous_ytd['Measure Names'] == '# of On Time Install')].groupby('Year')[
                'Measure Values'].sum()

            business_overall_ytd = business_previous_ytd.groupby('Year')['Measure Values'].sum()

            business_oti_ytd = business_on_time_ytd / business_overall_ytd * 100

            score_card = scorecard_functions.set_metric_value_previous(business_oti_ytd, index,
                                                                       score_card)

        elif scorecard_functions.is_modality_level(row=row, is_lcs=False):
            modality = scorecard_functions.get_modality_data(data=data, row=row, is_lcs=False,
                                                             is_previous_year=False,
                                                             number_of_weeks=0, metric=metric)

            modality_on_time = \
                modality.loc[modality['Measure Names'] == '# of On Time Install'].groupby('Year')[
                    'Measure Values'].sum()

            modality_overall = modality.groupby('Year')['Measure Values'].sum()

            modality_oti = modality_on_time / modality_overall * 100

            score_card = scorecard_functions.set_metric_value_current(modality_oti, index,
                                                                      score_card)

            modality_13_week = scorecard_functions.get_modality_data(data=data, row=row,
                                                                     is_lcs=False,
                                                                     is_previous_year=False,
                                                                     number_of_weeks=13,
                                                                     metric=metric)

            modality_on_time_13 = modality_13_week.loc[
                modality_13_week['Measure Names'] == '# of On Time Install'][
                'Measure Values'].sum()
            modality_overall_13 = modality_13_week['Measure Values'].sum()
            modality_oti_13 = modality_on_time_13 / modality_overall_13 * 100
            score_card = scorecard_functions.set_sc_column_value(modality_oti_13, index,
                                                                 "Rolling 13 weeks", score_card)

            modality_previous_ytd = scorecard_functions.get_modality_data(data=data, row=row,
                                                                          is_lcs=False,
                                                                          is_previous_year=True,
                                                                          number_of_weeks=0,
                                                                          metric=metric)

            modality_on_time_ytd = modality_previous_ytd.loc[
                modality_previous_ytd['Measure Names'] == '# of On Time Install'].groupby('Year')[
                'Measure Values'].sum()

            modality_overall_ytd = modality_previous_ytd.groupby('Year')['Measure Values'].sum()

            modality_oti_ytd = modality_on_time_ytd / modality_overall_ytd * 100

            score_card = scorecard_functions.set_metric_value_previous(modality_oti_ytd, index,
                                                                       score_card)
        elif scorecard_functions.is_modality_level(row=row, is_lcs=True):
            modality = scorecard_functions.get_modality_data(data=data, row=row, is_lcs=True,
                                                             is_previous_year=False,
                                                             number_of_weeks=0, metric=metric)

            modality_on_time = \
                modality.loc[modality['Measure Names'] == '# of On Time Install'].groupby('Year')[
                    'Measure Values'].sum()

            modality_overall = modality.groupby('Year')['Measure Values'].sum()

            modality_oti = modality_on_time / modality_overall * 100

            score_card = scorecard_functions.set_metric_value_current(modality_oti, index,
                                                                      score_card)
            modality_13_week = scorecard_functions.get_modality_data(data=data, row=row,
                                                                     is_lcs=True,
                                                                     is_previous_year=False,
                                                                     number_of_weeks=13,
                                                                     metric=metric)

            modality_on_time_13 = modality_13_week.loc[
                modality_13_week['Measure Names'] == '# of On Time Install'][
                'Measure Values'].sum()
            modality_overall_13 = modality_13_week['Measure Values'].sum()
            modality_oti_13 = modality_on_time_13 / modality_overall_13 * 100
            score_card = scorecard_functions.set_sc_column_value(modality_oti_13, index,
                                                                 "Rolling 13 weeks", score_card)

            modality_previous_ytd = scorecard_functions.get_modality_data(data=data, row=row,
                                                                          is_lcs=True,
                                                                          is_previous_year=True,
                                                                          number_of_weeks=0,
                                                                          metric=metric)

            modality_on_time_ytd = \
                modality_previous_ytd.loc[
                    modality_previous_ytd['Measure Names'] == '# of On Time Install'].groupby(
                    'Year')['Measure Values'].sum()

            modality_overall_ytd = modality_previous_ytd.groupby('Year')['Measure Values'].sum()

            modality_otd_ytd = modality_on_time_ytd / modality_overall_ytd * 100

            score_card = scorecard_functions.set_metric_value_previous(modality_otd_ytd, index,
                                                                       score_card)

        elif scorecard_functions.is_product_segment_level(row):
            prod_seg = scorecard_functions.get_product_segment_data(data, row, False, 0, metric)

            prod_seg_on_time = \
                prod_seg.loc[prod_seg['Measure Names'] == '# of On Time Install'].groupby('Year')[
                    'Measure Values'].sum()

            prod_seg_overall = prod_seg.groupby('Year')['Measure Values'].sum()

            prod_seg_oti = prod_seg_on_time / prod_seg_overall * 100

            score_card = scorecard_functions.set_metric_value_current(prod_seg_oti, index,
                                                                      score_card)
            prod_seg_13_week = scorecard_functions.get_product_segment_data(data, row, False, 13,
                                                                            metric)

            prod_seg_on_time_13 = \
                prod_seg_13_week.loc[
                    prod_seg_13_week['Measure Names'] == '# of On Time Install'][
                    'Measure Values'].sum()
            prod_seg_overall_13 = prod_seg_13_week['Measure Values'].sum()
            prod_seg_oti_13 = prod_seg_on_time_13 / prod_seg_overall_13 * 100

            score_card = scorecard_functions.set_sc_column_value(prod_seg_oti_13, index,
                                                                 "Rolling 13 weeks", score_card)
            prod_seg_ytd_previous = scorecard_functions.get_product_segment_data(data, row, True, 0,
                                                                                 metric)

            prod_seg_on_time_ytd = \
                prod_seg_ytd_previous.loc[
                    prod_seg['Measure Names'] == '# of On Time Install'].groupby(
                    'Year')['Measure Values'].sum()

            prod_seg_overall_ytd = prod_seg_ytd_previous.groupby('Year')['Measure Values'].sum()

            prod_seg_oti = prod_seg_on_time_ytd / prod_seg_overall_ytd * 100

            if len(prod_seg_oti.index.get_level_values(0)) > 0:
                score_card = scorecard_functions.set_metric_value_previous(prod_seg_oti, index,
                                                                           score_card)

        else:
            product = scorecard_functions.get_product_data(data, row, False, 0, metric)

            product_on_time = \
                product.loc[product['Measure Names'] == '# of On Time Install'].groupby('Year')[
                    'Measure Values'].sum()

            product_overall = product.groupby('Year')['Measure Values'].sum()

            product_oti = product_on_time / product_overall * 100

            score_card = scorecard_functions.set_metric_value_current(product_oti, index,
                                                                      score_card)

            product_13_week = scorecard_functions.get_product_data(data, row, False, 13, metric)

            product_on_time_13 = \
                product_13_week.loc[
                    product_13_week['Measure Names'] == '# of On Time Install'][
                    'Measure Values'].sum()
            product_overall_13 = product_13_week['Measure Values'].sum()
            product_oti_13 = product_on_time_13 / product_overall_13 * 100

            score_card = scorecard_functions.set_sc_column_value(product_oti_13, index,
                                                                 "Rolling 13 weeks", score_card)

            product_previous_ytd = scorecard_functions.get_product_data(data, row, True, 0, metric)

            product_on_time_ytd = product_previous_ytd.loc[
                product_previous_ytd['Measure Names'] == '# of On Time Install'].groupby('Year')[
                'Measure Values'].sum()

            product_overall_ytd = product_previous_ytd.groupby('Year')['Measure Values'].sum()

            product_oti_ytd = product_on_time_ytd / product_overall_ytd * 100

            if len(product_oti_ytd.index.get_level_values(0)) > 0:
                score_card = scorecard_functions.set_metric_value_previous(product_oti_ytd, index,
                                                                           score_card)
    return score_card


def calculate_cso(score_card, data):
    current_year = datetime.datetime.now().year
    current_month = datetime.datetime.now().strftime("%b")
    today = datetime.date.today()
    week_day = today.weekday()
    start_delta = datetime.timedelta(days=week_day, weeks=1)
    start_of_week = today - start_delta
    week_number = start_of_week.isocalendar()[1]
    years = data['Year'].unique()
    max_week_value_c = {}
    for year in years:
        year = int(year)
        max_week_lookup = data.loc[data["Year"] == year]
        max_week_value_c[year] = max_week_lookup.loc[max_week_lookup['Week'].idxmax()]['Week']
    for index, row in score_card.iterrows():
        if row['Product Quality Scorecard'] == 'GEHC':
            years = data['Year'].unique()
            for year in years:
                year = int(year)
                max_week_lookup = data.loc[data["Year"] == year]
                max_week_value_2 = \
                    max_week_lookup.loc[max_week_lookup['Week'].idxmax()][
                        'Week']
                red_cso_count = \
                    data[['Year', 'Week', 'Measure']].loc[
                        (data['Year'] == year)
                        & (data[
                               'Week'] == max_week_value_2)] \
                        .groupby('Year')['Measure'].sum()
                red_cso_count_2 = \
                    data[['Year', 'Week', 'Measure']].loc[
                        (data['Year'] == year)
                        & (data[
                               'Week'] == week_number)] \
                        .groupby('Year')['Measure'].sum()
                for value in red_cso_count.index.get_level_values(0):
                    if int(value) != current_year:
                        score_card.loc[
                            index, 'Dec ' + str(value) + "(FW " + str(max_week_value_2) + ")"] = \
                            red_cso_count[value]
                    else:
                        try:

                            score_card.loc[
                                index, str(current_month) + " " + str(value) + "(FW " + str(
                                    week_number) + ")"] = \
                                red_cso_count_2[value]
                        except IndexError:
                            score_card.loc[
                                index, str(current_month) + " " + str(value) + "(FW " + str(
                                    week_number) + ")"] = ""

                        previous_year_data = \
                            data[['Year', 'Week', 'Measure']].loc[
                                (data['Year'] == current_year - 1) &
                                (data['Week'] == week_number)]
                        if (previous_year_data.size > 0):
                            previous_year_ytd = \
                                previous_year_data.groupby('Year')[
                                    'Measure'].sum()
                            for value in previous_year_ytd.index.get_level_values(
                                    0):
                                score_card.loc[
                                    index, str(current_month) + " " + str(
                                        current_year - 1) + "(FW " + str(week_number) + ")"] = \
                                    previous_year_ytd[value]

        elif row['Product Quality Scorecard'] != 'GEHC' and row[
            'Business'] == 'ALL' and row['Modality'] == 'ALL':
            business = data.loc[data['SC Business Segment'] == row[
                'Product Quality Scorecard']]
            years = business['Year'].unique()
            for year in years:
                year = int(year)
                max_week_lookup = business.loc[business["Year"] == year]
                red_cso_count = business[['Year', 'Week', 'Measure']].loc[
                    (business['Year'] == year)
                    & (business['Week'] == max_week_value_c[year])] \
                    .groupby('Year')['Measure'].sum()
                red_cso_count_2 = business[['Year', 'Week', 'Measure']].loc[
                    (business['Year'] == year)
                    & (business['Week'] == week_number)] \
                    .groupby('Year')['Measure'].sum()
                for value in red_cso_count.index.get_level_values(0):
                    if int(value) != current_year:
                        score_card.loc[index, 'Dec ' + str(value) + "(FW " + str(
                            max_week_value_c[year]) + ")"] = \
                            red_cso_count[value]
                    else:
                        try:

                            score_card.loc[
                                index, str(current_month) + " " + str(value) + "(FW " + str(
                                    week_number) + ")"] = \
                                red_cso_count_2[value]
                        except IndexError:
                            score_card.loc[
                                index, str(current_month) + " " + str(value) + "(FW " + str(
                                    week_number) + ")"] = ""

                        previous_year_data = \
                            business[['Year', 'Week', 'Measure']].loc[
                                (business['Year'] == current_year - 1) &
                                (business['Week'] == week_number)]
                        if (previous_year_data.size > 0):
                            previous_year_ytd = \
                                previous_year_data.groupby('Year')[
                                    'Measure'].sum()
                            for value in previous_year_ytd.index.get_level_values(
                                    0):
                                score_card.loc[
                                    index, str(current_month) + " " + str(
                                        current_year - 1) + "(FW " + str(week_number) + ")"] = \
                                    previous_year_ytd[value]

        elif row['Business'] != 'ALL' and row['Modality'] == 'ALL' and row[
            'Product Quality Scorecard'] != 'LCS':
            modality = data[
                ['SC Business Segment', 'SC Modality', 'Week', 'Year',
                 'Measure']].loc[
                (data['SC Business Segment'] == row['Business']) & (
                        data['SC Modality'] == row['Product Quality Scorecard'])]
            years = modality['Year'].unique()
            for year in years:
                year = int(year)
                max_week_lookup = modality.loc[modality["Year"] == year]
                red_cso_count = modality[['Year', 'Week', 'Measure']].loc[
                    (modality['Year'] == year)
                    & (modality['Week'] == max_week_value_c[year])] \
                    .groupby('Year')['Measure'].sum()
                red_cso_count_2 = modality[['Year', 'Week', 'Measure']].loc[
                    (modality['Year'] == year)
                    & (modality['Week'] == week_number)] \
                    .groupby('Year')['Measure'].sum()
                for value in red_cso_count.index.get_level_values(0):
                    if int(value) != current_year:
                        score_card.loc[index, 'Dec ' + str(value) + "(FW " + str(
                            max_week_value_c[year]) + ")"] = \
                            red_cso_count[value]
                    else:
                        try:

                            score_card.loc[
                                index, str(current_month) + " " + str(value) + "(FW " + str(
                                    week_number) + ")"] = \
                                red_cso_count_2[value]
                        except IndexError:
                            score_card.loc[
                                index, str(current_month) + " " + str(value) + "(FW " + str(
                                    week_number) + ")"] = ""

                        previous_year_data = \
                            modality[['Year', 'Week', 'Measure']].loc[
                                (modality['Year'] == current_year - 1) &
                                (modality['Week'] == week_number)]
                        if (previous_year_data.size > 0):
                            previous_year_ytd = \
                                previous_year_data.groupby('Year')[
                                    'Measure'].sum()
                            for value in previous_year_ytd.index.get_level_values(
                                    0):
                                score_card.loc[
                                    index, str(current_month) + " " + str(
                                        current_year - 1) + "(FW " + str(week_number) + ")"] = \
                                    previous_year_ytd[value]
        elif row['Business'] != 'ALL' and row['Modality'] != 'ALL' and \
                row['Product Segment'] == 'ALL' \
                and row['Product Quality Scorecard'] != 'LCS':
            prod_seg = \
                data[['SC Business Segment', 'SC Modality',
                      'SC Product Segment',
                      'Year', 'Measure', 'Week']].loc[
                    (data['SC Business Segment'] == row['Business']) & (
                            data['SC Modality'] ==
                            row['Modality']) & (
                            data['SC Product Segment'] == row[
                        'Product Quality Scorecard'])]
            years = prod_seg['Year'].unique()
            for year in years:
                year = int(year)
                red_cso_count = prod_seg[['Year', 'Week', 'Measure']].loc[
                    (prod_seg['Year'] == year)
                    & (prod_seg['Week'] == max_week_value_c[year])] \
                    .groupby('Year')['Measure'].sum()
                red_cso_count_2 = prod_seg[['Year', 'Week', 'Measure']].loc[
                    (prod_seg['Year'] == year)
                    & (prod_seg['Week'] == week_number)] \
                    .groupby('Year')['Measure'].sum()
                for value in red_cso_count.index.get_level_values(0):
                    if int(value) != current_year:
                        score_card.loc[index, 'Dec ' + str(value) + "(FW " + str(
                            max_week_value_c[year]) + ")"] = \
                            red_cso_count[value]
                    else:
                        try:

                            score_card.loc[
                                index, str(current_month) + " " + str(value) + "(FW " + str(
                                    week_number) + ")"] = \
                                red_cso_count_2[value]
                        except IndexError:
                            score_card.loc[
                                index, str(current_month) + " " + str(value) + "(FW " + str(
                                    week_number) + ")"] = ""

                        previous_year_data = \
                            prod_seg[['Year', 'Week', 'Measure']].loc[
                                (prod_seg['Year'] == current_year - 1) &
                                (prod_seg['Week'] == week_number)]
                        if (previous_year_data.size > 0):
                            previous_year_ytd = \
                                previous_year_data.groupby('Year')[
                                    'Measure'].sum()
                            for value in previous_year_ytd.index.get_level_values(
                                    0):
                                score_card.loc[
                                    index, str(current_month) + " " + str(
                                        current_year - 1) + "(FW " + str(week_number) + ")"] = \
                                    previous_year_ytd[value]
        elif row['Business'] != 'ALL' and row['Modality'] == 'ALL' and row[
            'Product Quality Scorecard'] == 'LCS':
            prod_seg = \
                data[['SC Business Segment', 'SC Modality',
                      'SC Product Segment',
                      'Year', 'Measure', 'Week']].loc[
                    (data['SC Business Segment'] == row['Business'])
                    & (data['SC Modality'] != 'ULTRASOUND')
                    & (data['SC Modality'] != 'OTHER')
                    & (data['SC Modality'] != 'EXCLUDED')
                    & (row['Product Quality Scorecard'] == 'LCS')]
            years = prod_seg['Year'].unique()
            for year in years:
                year = int(year)
                max_week_lookup = prod_seg.loc[prod_seg["Year"] == year]
                red_cso_count = prod_seg[['Year', 'Week', 'Measure']].loc[
                    (prod_seg['Year'] == year)
                    & (prod_seg['Week'] == max_week_value_c[year])] \
                    .groupby('Year')['Measure'].sum()
                red_cso_count_2 = prod_seg[['Year', 'Week', 'Measure']].loc[
                    (prod_seg['Year'] == year)
                    & (prod_seg['Week'] == week_number)] \
                    .groupby('Year')['Measure'].sum()
                for value in red_cso_count.index.get_level_values(0):
                    if int(value) != current_year:
                        score_card.loc[index, 'Dec ' + str(value) + "(FW " + str(
                            max_week_value_c[year]) + ")"] = \
                            red_cso_count[value]
                    else:
                        try:
                            score_card.loc[
                                index, str(current_month) + " " + str(value) + "(FW " + str(
                                    week_number) + ")"] = \
                                red_cso_count_2[value]
                        except IndexError:
                            score_card.loc[
                                index, str(current_month) + " " + str(value) + "(FW " + str(
                                    week_number) + ")"] = ""
                        previous_year_data = \
                            prod_seg[['Year', 'Week', 'Measure']].loc[
                                (prod_seg['Year'] == current_year - 1) &
                                (prod_seg['Week'] == week_number)]
                        if (previous_year_data.size > 0):
                            previous_year_ytd = \
                                previous_year_data.groupby('Year')[
                                    'Measure'].sum()
                            for value in previous_year_ytd.index.get_level_values(
                                    0):
                                score_card.loc[
                                    index, str(current_month) + " " + str(
                                        current_year - 1) + "(FW " + str(week_number) + ")"] = \
                                    previous_year_ytd[value]

        else:
            product = \
                data[['SC Business Segment', 'SC Modality',
                      'SC Product Segment', 'SC Product',
                      'Year', 'Measure', 'Week']].loc[
                    (data['SC Business Segment'] == row['Business']) & (
                            data['SC Modality'] ==
                            row['Modality']) & (
                            data['SC Product Segment'] == row['Product Segment']) &
                    (data['SC Product'] == row['Product Quality Scorecard'])]
            years = product['Year'].unique()
            for year in years:
                year = int(year)
                max_week_lookup = product.loc[product["Year"] == year]
                red_cso_count = product[['Year', 'Week', 'Measure']].loc[
                    (product['Year'] == year)
                    & (product['Week'] == max_week_value_c[year])] \
                    .groupby('Year')['Measure'].sum()
                red_cso_count_2 = product[['Year', 'Week', 'Measure']].loc[
                    (product['Year'] == year)
                    & (product['Week'] == week_number)] \
                    .groupby('Year')['Measure'].sum()
                for value in red_cso_count.index.get_level_values(0):
                    if int(value) != current_year:
                        score_card.loc[index, 'Dec ' + str(value) + "(FW " + str(
                            max_week_value_c[year]) + ")"] = \
                            red_cso_count[value]
                    else:
                        try:
                            score_card.loc[
                                index, str(current_month) + " " + str(value) + "(FW " + str(
                                    week_number) + ")"] = \
                                red_cso_count_2[value]
                        except IndexError:
                            score_card.loc[
                                index, str(current_month) + " " + str(value) + "(FW " + str(
                                    week_number) + ")"] = ""

                        previous_year_data = \
                            product[['Year', 'Week', 'Measure']].loc[
                                (product['Year'] == current_year - 1) &
                                (product['Week'] == week_number)]
                        if (previous_year_data.size > 0):
                            previous_year_ytd = \
                                previous_year_data.groupby('Year')[
                                    'Measure'].sum()
                            for value in previous_year_ytd.index.get_level_values(
                                    0):
                                score_card.loc[
                                    index, str(current_month) + " " + str(
                                        current_year - 1) + "(FW " + str(week_number) + ")"] = \
                                    previous_year_ytd[value]
    return score_card


def calculate_cso_total(score_card, data):
    current_year = datetime.datetime.now().year
    current_month = datetime.datetime.now().strftime("%b")
    today = datetime.date.today()
    weekday = today.weekday()
    start_delta = datetime.timedelta(days=weekday, weeks=1)
    start_of_week = today - start_delta
    week_number = start_of_week.isocalendar()[1]
    max_week_value_c = {}
    years = data['Year'].unique()
    for year in years:
        year = int(year)
        max_week_lookup = data.loc[data["Year"] == year]
        max_week_value_c[year] = max_week_lookup.loc[max_week_lookup['Week'].idxmax()]['Week']
    for index, row in score_card.iterrows():
        if row['Product Quality Scorecard'] == 'GEHC':
            years = data['Year'].unique()
            for year in years:
                year = int(year)
                max_week_lookup = data.loc[data["Year"] == year]
                max_week_value = \
                    max_week_lookup.loc[max_week_lookup['Week'].idxmax()][
                        'Week']

                red_cso_count = \
                    data[['Year', 'Week', 'Measure']].loc[
                        (data['Year'] == year)
                        & (data[
                               'Week'] == max_week_value)] \
                        .groupby('Year')['Measure'].sum()
                red_cso_count_2 = \
                    data[['Year', 'Week', 'Measure']].loc[
                        (data['Year'] == year)
                        & (data[
                               'Week'] == week_number)] \
                        .groupby('Year')['Measure'].sum()
                for value in red_cso_count.index.get_level_values(0):
                    if int(value) != current_year:
                        score_card.loc[
                            index, 'Dec ' + str(value) + "(FW " + str(max_week_value) + ")"] = \
                            red_cso_count[value]
                    else:
                        try:

                            score_card.loc[
                                index, str(current_month) + " " + str(value) + "(FW " + str(
                                    week_number) + ")"] = \
                                red_cso_count_2[value]
                        except IndexError:
                            score_card.loc[
                                index, str(current_month) + " " + str(value) + "(FW " + str(
                                    week_number) + ")"] = ""

                        previous_year_data = \
                            data[['Year', 'Week', 'Measure']].loc[
                                (data['Year'] == current_year - 1) &
                                (data['Week'] == week_number)]
                        if (previous_year_data.size > 0):
                            previous_year_ytd = \
                                previous_year_data.groupby('Year')[
                                    'Measure'].sum()
                            for value in previous_year_ytd.index.get_level_values(
                                    0):
                                score_card.loc[
                                    index, str(current_month) + " " + str(
                                        current_year - 1) + "(FW " + str(week_number) + ")"] = \
                                    previous_year_ytd[value]
        elif row['Product Quality Scorecard'] != 'GEHC' and row[
            'Business'] == 'ALL' and row['Modality'] == 'ALL':
            business = data.loc[data['SC Business Segment'] == row[
                'Product Quality Scorecard']]
            years = business['Year'].unique()
            for year in years:
                year = int(year)
                max_week_lookup = business.loc[business["Year"] == year]
                max_week_value = \
                    max_week_lookup.loc[max_week_lookup['Week'].idxmax()][
                        'Week']

                red_cso_count = business[['Year', 'Week', 'Measure']].loc[
                    (business['Year'] == year)
                    & (business['Week'] == max_week_value)] \
                    .groupby('Year')['Measure'].sum()
                red_cso_count_2 = business[['Year', 'Week', 'Measure']].loc[
                    (business['Year'] == year)
                    & (business['Week'] == week_number)] \
                    .groupby('Year')['Measure'].sum()
                for value in red_cso_count.index.get_level_values(0):
                    if int(value) != current_year:
                        score_card.loc[index, 'Dec ' + str(value) + "(FW " + str(
                            max_week_value_c[year]) + ")"] = \
                            red_cso_count[value]
                    else:
                        try:

                            score_card.loc[
                                index, str(current_month) + " " + str(value) + "(FW " + str(
                                    week_number) + ")"] = \
                                red_cso_count_2[value]
                        except IndexError:
                            score_card.loc[
                                index, str(current_month) + " " + str(value) + "(FW " + str(
                                    week_number) + ")"] = ""

                        previous_year_data = \
                            business[['Year', 'Week', 'Measure']].loc[
                                (business['Year'] == current_year - 1) &
                                (business['Week'] == week_number)]
                        if (previous_year_data.size > 0):
                            previous_year_ytd = \
                                previous_year_data.groupby('Year')[
                                    'Measure'].sum()
                            for value in previous_year_ytd.index.get_level_values(
                                    0):
                                score_card.loc[
                                    index, str(current_month) + " " + str(
                                        current_year - 1) + "(FW " + str(week_number) + ")"] = \
                                    previous_year_ytd[value]
        elif row['Business'] != 'ALL' and row['Modality'] == 'ALL' and row[
            'Product Quality Scorecard'] != 'LCS':
            modality = data[
                ['SC Business Segment', 'SC Modality', 'Week', 'Year',
                 'Measure']].loc[
                (data['SC Business Segment'] == row['Business']) & (
                        data['SC Modality'] == row['Product Quality Scorecard'])]
            years = modality['Year'].unique()
            for year in years:
                year = int(year)
                max_week_lookup = modality.loc[modality["Year"] == year]
                max_week_value = \
                    max_week_lookup.loc[max_week_lookup['Week'].idxmax()][
                        'Week']
                red_cso_count = modality[['Year', 'Week', 'Measure']].loc[
                    (modality['Year'] == year)
                    & (modality['Week'] == max_week_value)] \
                    .groupby('Year')['Measure'].sum()
                red_cso_count_2 = modality[['Year', 'Week', 'Measure']].loc[
                    (modality['Year'] == year)
                    & (modality['Week'] == week_number)] \
                    .groupby('Year')['Measure'].sum()
                for value in red_cso_count.index.get_level_values(0):
                    if int(value) != current_year:
                        score_card.loc[index, 'Dec ' + str(value) + "(FW " + str(
                            max_week_value_c[year]) + ")"] = \
                            red_cso_count[value]
                    else:
                        try:

                            score_card.loc[
                                index, str(current_month) + " " + str(value) + "(FW " + str(
                                    week_number) + ")"] = \
                                red_cso_count_2[value]
                        except IndexError:
                            score_card.loc[
                                index, str(current_month) + " " + str(value) + "(FW " + str(
                                    week_number) + ")"] = ""

                        previous_year_data = \
                            modality[['Year', 'Week', 'Measure']].loc[
                                (modality['Year'] == current_year - 1) &
                                (modality['Week'] == week_number)]
                        if (previous_year_data.size > 0):
                            previous_year_ytd = \
                                previous_year_data.groupby('Year')[
                                    'Measure'].sum()
                            for value in previous_year_ytd.index.get_level_values(
                                    0):
                                score_card.loc[
                                    index, str(current_month) + " " + str(
                                        current_year - 1) + "(FW " + str(week_number) + ")"] = \
                                    previous_year_ytd[value]
        elif row['Business'] != 'ALL' and row['Modality'] != 'ALL' and \
                row['Product Segment'] == 'ALL' \
                and row['Product Quality Scorecard'] != 'LCS':
            prod_seg = \
                data[['SC Business Segment', 'SC Modality',
                      'SC Product Segment',
                      'Year', 'Measure', 'Week']].loc[
                    (data['SC Business Segment'] == row['Business']) & (
                            data['SC Modality'] ==
                            row['Modality']) & (
                            data['SC Product Segment'] == row[
                        'Product Quality Scorecard'])]
            years = prod_seg['Year'].unique()
            for year in years:
                year = int(year)
                max_week_lookup = prod_seg.loc[prod_seg["Year"] == year]
                max_week_value = \
                    max_week_lookup.loc[max_week_lookup['Week'].idxmax()][
                        'Week']
                red_cso_count = prod_seg[['Year', 'Week', 'Measure']].loc[
                    (prod_seg['Year'] == year)
                    & (prod_seg['Week'] == max_week_value)] \
                    .groupby('Year')['Measure'].sum()
                red_cso_count_2 = prod_seg[['Year', 'Week', 'Measure']].loc[
                    (prod_seg['Year'] == year)
                    & (prod_seg['Week'] == week_number)] \
                    .groupby('Year')['Measure'].sum()
                for value in red_cso_count.index.get_level_values(0):
                    if int(value) != current_year:
                        score_card.loc[index, 'Dec ' + str(value) + "(FW " + str(
                            max_week_value_c[year]) + ")"] = \
                            red_cso_count[value]
                    else:
                        try:

                            score_card.loc[
                                index, str(current_month) + " " + str(value) + "(FW " + str(
                                    week_number) + ")"] = \
                                red_cso_count_2[value]
                        except IndexError:
                            score_card.loc[
                                index, str(current_month) + " " + str(value) + "(FW " + str(
                                    week_number) + ")"] = ""

                        previous_year_data = \
                            prod_seg[['Year', 'Week', 'Measure']].loc[
                                (prod_seg['Year'] == current_year - 1) &
                                (prod_seg['Week'] == week_number)]
                        if (previous_year_data.size > 0):
                            previous_year_ytd = \
                                previous_year_data.groupby('Year')[
                                    'Measure'].sum()
                            for value in previous_year_ytd.index.get_level_values(
                                    0):
                                score_card.loc[
                                    index, str(current_month) + " " + str(
                                        current_year - 1) + "(FW " + str(week_number) + ")"] = \
                                    previous_year_ytd[value]
        elif row['Business'] != 'ALL' and row['Modality'] == 'ALL' and row[
            'Product Quality Scorecard'] == 'LCS':
            prod_seg = \
                data[['SC Business Segment', 'SC Modality',
                      'SC Product Segment',
                      'Year', 'Measure', 'Week']].loc[
                    (data['SC Business Segment'] == row['Business'])
                    & (data['SC Modality'] != 'ULTRASOUND')
                    & (data['SC Modality'] != 'OTHER')
                    & (data['SC Modality'] != 'EXCLUDED')
                    & (row['Product Quality Scorecard'] == 'LCS')]
            years = prod_seg['Year'].unique()
            for year in years:
                year = int(year)
                max_week_lookup = prod_seg.loc[prod_seg["Year"] == year]
                max_week_value = \
                    max_week_lookup.loc[max_week_lookup['Week'].idxmax()][
                        'Week']
                red_cso_count = prod_seg[['Year', 'Week', 'Measure']].loc[
                    (prod_seg['Year'] == year)
                    & (prod_seg['Week'] == max_week_value)] \
                    .groupby('Year')['Measure'].sum()
                red_cso_count_2 = prod_seg[['Year', 'Week', 'Measure']].loc[
                    (prod_seg['Year'] == year)
                    & (prod_seg['Week'] == week_number)] \
                    .groupby('Year')['Measure'].sum()
                for value in red_cso_count.index.get_level_values(0):
                    if int(value) != current_year:
                        score_card.loc[index, 'Dec ' + str(value) + "(FW " + str(
                            max_week_value_c[year]) + ")"] = \
                            red_cso_count[value]
                    else:
                        try:
                            score_card.loc[
                                index, str(current_month) + " " + str(value) + "(FW " + str(
                                    week_number) + ")"] = \
                                red_cso_count_2[value]
                        except IndexError:
                            score_card.loc[
                                index, str(current_month) + " " + str(value) + "(FW " + str(
                                    week_number) + ")"] = ""
                        previous_year_data = \
                            prod_seg[['Year', 'Week', 'Measure']].loc[
                                (prod_seg['Year'] == current_year - 1) &
                                (prod_seg['Week'] == week_number)]
                        if (previous_year_data.size > 0):
                            previous_year_ytd = \
                                previous_year_data.groupby('Year')[
                                    'Measure'].sum()
                            for value in previous_year_ytd.index.get_level_values(
                                    0):
                                score_card.loc[
                                    index, str(current_month) + " " + str(
                                        current_year - 1) + "(FW " + str(week_number) + ")"] = \
                                    previous_year_ytd[value]

        else:
            product = \
                data[['SC Business Segment', 'SC Modality',
                      'SC Product Segment', 'SC Product',
                      'Year', 'Measure', 'Week']].loc[
                    (data['SC Business Segment'] == row['Business']) & (
                            data['SC Modality'] ==
                            row['Modality']) & (
                            data['SC Product Segment'] == row['Product Segment']) &
                    (data['SC Product'] == row['Product Quality Scorecard'])]
            years = product['Year'].unique()
            for year in years:
                year = int(year)
                max_week_lookup = product.loc[product["Year"] == year]
                max_week_value = \
                    max_week_lookup.loc[max_week_lookup['Week'].idxmax()][
                        'Week']

                red_cso_count = product[['Year', 'Week', 'Measure']].loc[
                    (product['Year'] == year)
                    & (product['Week'] == max_week_value)] \
                    .groupby('Year')['Measure'].sum()
                red_cso_count_2 = product[['Year', 'Week', 'Measure']].loc[
                    (product['Year'] == year)
                    & (product['Week'] == week_number)] \
                    .groupby('Year')['Measure'].sum()
                for value in red_cso_count.index.get_level_values(0):
                    if int(value) != current_year:
                        score_card.loc[index, 'Dec ' + str(value) + "(FW " + str(
                            max_week_value_c[year]) + ")"] = \
                            red_cso_count[value]
                    else:
                        try:
                            score_card.loc[
                                index, str(current_month) + " " + str(value) + "(FW " + str(
                                    week_number) + ")"] = \
                                red_cso_count_2[value]
                        except IndexError:
                            score_card.loc[
                                index, str(current_month) + " " + str(value) + "(FW " + str(
                                    week_number) + ")"] = ""
                        previous_year_data = \
                            product[['Year', 'Week', 'Measure']].loc[
                                (product['Year'] == current_year - 1) &
                                (product['Week'] == week_number)]
                        if (previous_year_data.size > 0):
                            previous_year_ytd = \
                                previous_year_data.groupby('Year')[
                                    'Measure'].sum()
                            for value in previous_year_ytd.index.get_level_values(
                                    0):
                                score_card.loc[
                                    index, str(current_month) + " " + str(
                                        current_year - 1) + "(FW " + str(week_number) + ")"] = \
                                    previous_year_ytd[value]

    return score_card


def setup_chu_hierarchy(data, chu_hierarachy):
    data['Modality'].fillna('', inplace=True)
    data['Measure Values'].fillna(0, inplace=True)
    data['Measure Values'] = data['Measure Values'].str.replace(",", "")
    data['Measure Values'].fillna(0, inplace=True)
    data['Measure Values'] = data['Measure Values'].astype(int)
    data['Product Group / Modality Segment - IB'].fillna('', inplace=True)

    data['lookup'] = data['Modality'] + data[
        'Product Group / Modality Segment - IB']
    time_labels = data['By Time Label'].unique()
    for time_label in time_labels:
        data.loc[data['By Time Label'] == time_label, "Year"] = \
            time_label.split('M')[0]
        data.loc[data['By Time Label'] == time_label, "Month"] = \
            time_label.split('M')[1]
    data['Year'] = data['Year'].astype(int)
    data['Month'] = data['Month'].astype(int)
    modalities = data['Modality'].unique()
    for modality in modalities:
        chu_modality_data = chu_hierarachy.loc[
            chu_hierarachy['Modality'] == modality]
        if chu_modality_data.size > 0:
            data.loc[data['Modality'] == modality, 'SC Business Segment'] = \
                chu_modality_data['Business for Scorecard'] \
                    .values[0]
            data.loc[data['Modality'] == modality, 'SC Modality'] = \
                chu_modality_data['Modality for Scorecard'] \
                    .values[0]
            prod_segments = data.loc[data['Modality'] == modality][
                'lookup'].unique()
            for prod_segment in prod_segments:
                prod_segment_data = chu_hierarachy.loc[
                    chu_hierarachy['Lookup Value'] == prod_segment]
                if prod_segment_data.size > 0:
                    data.loc[(data['Modality'] == modality) & (
                            data[
                                'lookup'] == prod_segment), 'SC Product Segment'] = \
                        prod_segment_data[
                            'Product Segment for Scorecard'].values[0]
                    data.loc[(data['Modality'] == modality) & (
                            data['lookup'] == prod_segment), 'SC Product'] = \
                        prod_segment_data[
                            'Modality Segment / Product Group'].values[0]
                else:
                    data.loc[(data['Modality'] == modality) & (
                            data[
                                'lookup'] == prod_segment), 'SC Product Segment'] = \
                        'OTHER'
        else:
            data.loc[data[
                         'Modality'] == modality, 'SC Business Segment'] = 'GEHC OTHER'
            data.loc[data['Modality'] == modality, 'SC Modality'] = 'OTHER'
            data.loc[
                data['Modality'] == modality, 'SC Product Segment'] = 'OTHER'

    data['SC Product'].fillna('', inplace=True)
    blank_scorecard_products = data.loc[data['SC Product'] == '']
    if blank_scorecard_products.size > 0:
        products = data.loc[data['SC Product'] == ''][
            'Product Group / Modality Segment - IB'].unique()
        for product in products:
            scorecard_product = ''
            if product == '':
                scorecard_product = '<blank in source>'
            else:
                scorecard_product = product
            data.loc[(data['SC Product'] == '') & (
                    data['Product Group / Modality Segment - IB'] == product),
                     'SC Product'] = scorecard_product
    return data


def setup_obf_hierarchy(data, sii, metric_name):
    if metric_name == 'OBF':
        data.rename(
            columns={'Month': 'month_name', 'Refresh Date': 'Refresh Date',
                     'SII Modality': 'SII Modality Code',
                     'SII Product Group': 'SII Product Group',
                     'SII Sub Modality': 'Sub Modality Code',
                     'Year': 'Year of Process Date',
                     '# of SRs': 'Measured Value'}, inplace=True)
    if metric_name == 'FOI':
        time_labels = data['Applied Month'].unique()
        for time_label in time_labels:
            if time_label is not None and len(str(time_label)) > 0:
                data.loc[data['Applied Month'] == time_label, "Year"] = \
                    str(time_label)[:4]
                if len(str(time_label)) == 6:
                    data.loc[data['Applied Month'] == time_label, 'Month'] = \
                        str(time_label)[-2:]
                if len(str(time_label)) == 5:
                    data.loc[data['Applied Month'] == time_label, 'Month'] = \
                        str(time_label)[-1:]
        data.rename(
            columns={'Refresh Date': 'Refresh Date',
                     'Modality Code': 'SII Modality Code',
                     'Product Group / Family': 'SII Product Group',
                     'Business Segment Code': 'Sub Modality Code',
                     'Year': 'Year of Process Date',
                     'PPI New': 'Measured Value'}, inplace=True)
    data['SII Modality Code'].fillna("", inplace=True)
    data['Sub Modality Code'].fillna("", inplace=True)
    data['SII Product Group'].fillna("", inplace=True)
    data['Year of Process Date'] = data['Year of Process Date'].astype(int)
    if metric_name == 'OBF':
        abbr_to_num = {name: num for num, name in
                       enumerate(calendar.month_name) if
                       num}
        months = data['month_name'].unique()
        for month in months:
            data.loc[data['month_name'] == month, 'Month'] = abbr_to_num[month]
    data['Month'] = data['Month'].astype(int)
    data['look_up'] = data['SII Modality Code'] + data['Sub Modality Code'] + \
                      data['SII Product Group']
    lookup_values = data['look_up'].unique()
    for lookup_value in lookup_values:
        sii_data = sii.loc[sii['lookup value'].apply(
            lambda x: x.lower()) == lookup_value.lower()]
        if sii_data.size > 0:
            data.loc[data['look_up'] == lookup_value, 'SC Business Segment'] = \
                sii_data['Business'].values[0]
            data.loc[data['look_up'] == lookup_value, 'SC Modality'] = \
                sii_data['Modality'].values[0]
            data.loc[data['look_up'] == lookup_value, 'SC Product Segment'] = \
                sii_data['Product Segment'].values[0]
            data.loc[data['look_up'] == lookup_value, 'SC Product'] = \
                sii_data['sub_family_code (Ultrasound Only) ' \
                         'or product_group_code'].values[0]
        else:
            data.loc[data[
                         'look_up'] == lookup_value, 'SC Business Segment'] = 'GEHC OTHER'
            data.loc[data['look_up'] == lookup_value, 'SC Modality'] = 'OTHER'
            data.loc[data[
                         'look_up'] == lookup_value, 'SC Product Segment'] = 'OTHER'
            data.loc[data['look_up'] == lookup_value, 'SC Product'] = \
                data.loc[data['look_up'] == lookup_value] \
                    ['SII Product Group']
    current_year = datetime.datetime.now().year
    current_month = datetime.datetime.now().month
    today = datetime.date.today()
    weekday = today.weekday()
    start_delta = datetime.timedelta(days=weekday, weeks=1)
    start_of_week = today - start_delta
    week_number = start_of_week.isocalendar()[1]

    if metric_name != "FOI":
        data = data.loc[(data['Year of Process Date'] < current_year) | (
                (data['Year of Process Date'] == current_year)
                & (data['Month'] <= current_month) & (data["Week Number"] <= week_number))]
    else:
        data = data.loc[(data['Year of Process Date'] < current_year) | (
                (data['Year of Process Date'] == current_year)
                & (data['Month'] <= current_month))]

    data['SC Product'].fillna('', inplace=True)
    blank_scorecard_products = data.loc[data['SC Product'] == '']
    if blank_scorecard_products.size > 0:
        products = data.loc[data['SC Product'] == ''][
            'SII Product Group'].unique()
        for product in products:
            scorecard_product = ''
            if product == '':
                scorecard_product = '<BLANK IN SOURCE>'
            else:
                scorecard_product = product

            data.loc[(data['SC Product'] == '') & (
                    data['SII Product Group'] == product), 'SC Product'] = \
                scorecard_product
    return data


def calculate_obf(score_card, data):
    current_year = datetime.datetime.now().year
    today = datetime.date.today()
    weekday = today.weekday()
    start_delta = datetime.timedelta(days=weekday, weeks=1)
    start_of_week = today - start_delta
    week_number = start_of_week.isocalendar()[1]
    for index, row in score_card.iterrows():
        if row['Product Quality Scorecard'] == 'GEHC':
            gehc = data.groupby('Year of Process Date')['Measured Value'].sum()

            for value in gehc.index.get_level_values(0):
                if int(value) != current_year:
                    score_card.loc[index, str(value) + ' Total'] = gehc[value]
                else:
                    score_card.loc[index, str(value) + ' YTD'] = gehc[value]
            gehc_ytd = \
                data[['Measured Value', 'Year of Process Date', 'Week Number']].loc[
                    (data['Year of Process Date'] == current_year - 1) & (
                            data['Week Number'] <= week_number)]
            gehc_ytd_group = gehc_ytd.groupby('Year of Process Date')[
                'Measured Value'].sum()
            if gehc_ytd_group.size > 0:
                score_card.loc[index, str(
                    gehc_ytd_group.index.get_level_values(0)[0]) + ' YTD'] = \
                    gehc_ytd_group[gehc_ytd_group.index.get_level_values(0)[0]]

        elif row['Product Quality Scorecard'] != 'GEHC' and row[
            'Business'] == 'ALL' and row['Modality'] == 'ALL':
            business = data.loc[data['SC Business Segment'] == row[
                'Product Quality Scorecard']]
            business_group = business.groupby('Year of Process Date')[
                'Measured Value'].sum()
            for value in business_group.index.get_level_values(0):
                if value != current_year:
                    score_card.loc[index, str(value) + ' Total'] = \
                        business_group[value]
                else:
                    score_card.loc[index, str(value) + ' YTD'] = \
                        business_group[value]
            business_ytd = \
                business[
                    ['Measured Value', 'Year of Process Date', 'Week Number']].loc[
                    (business['Year of Process Date'] == current_year - 1) & (
                            business['Week Number'] <= week_number)]
            business_ytd_group = business_ytd.groupby('Year of Process Date')[
                'Measured Value'].sum()
            if business_ytd_group.size > 0:
                score_card.loc[index, str(
                    business_ytd_group.index.get_level_values(0)[
                        0]) + ' YTD'] = \
                    business_ytd_group[
                        business_ytd_group.index.get_level_values(0)[0]]
        elif row['Business'] != 'ALL' and row['Modality'] == 'ALL' and row[
            'Product Quality Scorecard'] != 'LCS':
            modality = \
                data[['SC Business Segment', 'SC Modality', 'Measured Value',
                      'Year of Process Date', 'Week Number']].loc[
                    (data['SC Business Segment'] == row['Business']) & (
                            data['SC Modality'] == row[
                        'Product Quality Scorecard'])]
            modality_group = modality.groupby('Year of Process Date')[
                'Measured Value'].sum()
            for value in modality_group.index.get_level_values(0):
                if value != current_year:
                    score_card.loc[index, str(value) + ' Total'] = \
                        modality_group[value]
                else:
                    score_card.loc[index, str(value) + ' YTD'] = \
                        modality_group[value]
            modality_ytd = \
                modality[
                    ['Measured Value', 'Year of Process Date', 'Week Number']].loc[
                    (modality['Year of Process Date'] == current_year - 1) & (
                            modality['Week Number'] <= week_number)]
            modality_ytd_group = modality_ytd.groupby('Year of Process Date')[
                'Measured Value'].sum()
            if modality_ytd_group.size > 0:
                score_card.loc[index, str(
                    modality_ytd_group.index.get_level_values(0)[
                        0]) + ' YTD'] = \
                    modality_ytd_group[
                        modality_ytd_group.index.get_level_values(0)[0]]
        elif row['Business'] != 'ALL' and row['Modality'] == 'ALL' and row[
            'Product Quality Scorecard'] == 'LCS':
            modality = \
                data[['SC Business Segment', 'SC Modality', 'Measured Value',
                      'Year of Process Date', 'Week Number']].loc[
                    (data['SC Business Segment'] == row['Business'])
                    & (data['SC Modality'] != 'ULTRASOUND')
                    & (data['SC Modality'] != 'EXCLUDED')
                    & (data['SC Modality'] != 'OTHER')]
            modality_group = modality.groupby('Year of Process Date')[
                'Measured Value'].sum()
            for value in modality_group.index.get_level_values(0):
                if value != current_year:
                    score_card.loc[index, str(value) + ' Total'] = \
                        modality_group[value]
                else:
                    score_card.loc[index, str(value) + ' YTD'] = \
                        modality_group[value]
            modality_ytd = \
                modality[
                    ['Measured Value', 'Year of Process Date', 'Week Number']].loc[
                    (modality['Year of Process Date'] == current_year - 1) & (
                            modality['Week Number'] <= week_number)]
            modality_ytd_group = modality_ytd.groupby('Year of Process Date')[
                'Measured Value'].sum()
            if modality_ytd_group.size > 0:
                score_card.loc[index, str(
                    modality_ytd_group.index.get_level_values(0)[
                        0]) + ' YTD'] = \
                    modality_ytd_group[
                        modality_ytd_group.index.get_level_values(0)[0]]
        elif row['Business'] != 'ALL' and row['Modality'] != 'ALL' and row[
            'Product Segment'] == 'ALL':
            prod_seg = \
                data[['SC Business Segment', 'SC Modality',
                      'SC Product Segment', 'Measured Value',
                      'Year of Process Date', 'Week Number']].loc[
                    (data['SC Business Segment'] == row['Business']) & (
                            data['SC Modality'] == row['Modality']) & (
                            data['SC Product Segment'] == row[
                        'Product Quality Scorecard'])]
            prod_seg_group = prod_seg.groupby('Year of Process Date')[
                'Measured Value'].sum()
            for value in prod_seg_group.index.get_level_values(0):
                if value != current_year:
                    score_card.loc[index, str(value) + ' Total'] = \
                        prod_seg_group[value]
                else:
                    score_card.loc[index, str(value) + ' YTD'] = \
                        prod_seg_group[value]
            try:
                prod_seg_ytd = prod_seg[
                    ['Measured Value', 'Year of Process Date', 'Week Number']].loc[
                    (prod_seg['Year of Process Date'] == current_year - 1) & (
                            prod_seg['Week Number'] <= week_number)]

                prod_seg_ytd_group = \
                    prod_seg_ytd.groupby('Year of Process Date')[
                        'Measured Value'].sum()

                if prod_seg_ytd_group.size > 0:
                    score_card.loc[index, str(
                        prod_seg_ytd_group.index.get_level_values(0)[
                            0]) + ' YTD'] = \
                        prod_seg_ytd_group[
                            prod_seg_ytd_group.index.get_level_values(0)[0]]
            except KeyError:
                score_card.loc[index, str(current_year - 1) + ' YTD'] = ''
            except IndexError:
                score_card.loc[index, str(current_year - 1) + ' YTD'] = ''
        else:
            product = \
                data[['SC Business Segment', 'SC Modality',
                      'SC Product Segment', 'SC Product', 'Measured Value',
                      'Year of Process Date', 'Week Number']].loc[
                    (data['SC Business Segment'] == row['Business']) & (
                            data['SC Modality'] == row['Modality']) & (
                            data['SC Product Segment'] == row['Product Segment'])
                    & (data['SC Product'] == row['Product Quality Scorecard'])]
            product_group = product.groupby('Year of Process Date')[
                'Measured Value'].sum()
            for value in product_group.index.get_level_values(0):
                if value != current_year:
                    score_card.loc[index, str(value) + ' Total'] = \
                        product_group[value]
                else:
                    score_card.loc[index, str(value) + ' YTD'] = product_group[
                        value]
            try:
                product_ytd = product[
                    ['Measured Value', 'Year of Process Date', 'Week Number']].loc[
                    (product['Year of Process Date'] == current_year - 1) & (
                            product['Week Number'] <= week_number)]

                product_ytd_group = \
                    product_ytd.groupby('Year of Process Date')[
                        'Measured Value'].sum()

                if product_ytd_group.size > 0:
                    score_card.loc[index, str(
                        product_ytd_group.index.get_level_values(0)[
                            0]) + ' YTD'] = \
                        product_ytd_group[
                            product_ytd_group.index.get_level_values(0)[0]]
            except KeyError:
                score_card.loc[index, str(current_year - 1) + ' YTD'] = ''
            except IndexError:
                score_card.loc[index, str(current_year - 1) + ' YTD'] = ''
    return score_card


def calculate_elf(score_card, data):
    current_year = datetime.datetime.now().year
    today = datetime.date.today()
    weekday = today.weekday()
    start_delta = datetime.timedelta(days=weekday, weeks=1)
    start_of_week = today - start_delta
    week_number = start_of_week.isocalendar()[1]
    for index, row in score_card.iterrows():
        if row['Product Quality Scorecard'] == 'GEHC':
            gehc = data.groupby('Year of service_close_date')[
                'TOTALGECOST'].sum()

            for value in gehc.index.get_level_values(0):
                if int(value) != current_year:
                    score_card.loc[index, str(value) + ' Total'] = gehc[value]
                else:
                    score_card.loc[index, str(value) + ' YTD'] = gehc[value]
            gehc_ytd = data[['TOTALGECOST', 'Year of service_close_date',
                             'Week Number']].loc[
                (data['Year of service_close_date'] == current_year - 1) & (
                        data['Week Number'] <= week_number)]
            gehc_ytd_group = gehc_ytd.groupby('Year of service_close_date')[
                'TOTALGECOST'].sum()
            if len(gehc_ytd_group) > 0:
                score_card.loc[index, str(
                    gehc_ytd_group.index.get_level_values(0)[0]) + ' YTD'] = \
                    gehc_ytd_group[gehc_ytd_group.index.get_level_values(0)[0]]

        elif row['Product Quality Scorecard'] != 'GEHC' and row[
            'Business'] == 'ALL' and row['Modality'] == 'ALL':
            business = data.loc[data['SC Business Segment'] == row[
                'Product Quality Scorecard']]
            business_group = business.groupby('Year of service_close_date')[
                'TOTALGECOST'].sum()
            for value in business_group.index.get_level_values(0):
                if value != current_year:
                    score_card.loc[index, str(value) + ' Total'] = \
                        business_group[value]
                else:
                    score_card.loc[index, str(value) + ' YTD'] = \
                        business_group[value]
            business_ytd = business[
                ['TOTALGECOST', 'Year of service_close_date',
                 'Week Number']].loc[
                (
                        business[
                            'Year of service_close_date'] == current_year - 1) & (
                        business['Week Number'] <= week_number)]
            business_ytd_group = \
                business_ytd.groupby('Year of service_close_date')[
                    'TOTALGECOST'].sum()
            if len(business_ytd_group) > 0:
                score_card.loc[index, str(
                    business_ytd_group.index.get_level_values(0)[0]) + ' YTD'] = \
                    business_ytd_group[
                        business_ytd_group.index.get_level_values(0)[0]]
        elif row['Business'] != 'ALL' and row['Modality'] == 'ALL' and row[
            'Product Quality Scorecard'] != 'LCS':
            modality = \
                data[['SC Business Segment', 'SC Modality', 'TOTALGECOST',
                      'Year of service_close_date',
                      'Week Number']].loc[
                    (data['SC Business Segment'] == row['Business']) & (
                            data['SC Modality'] == row[
                        'Product Quality Scorecard'])]
            modality_group = modality.groupby('Year of service_close_date')[
                'TOTALGECOST'].sum()
            for value in modality_group.index.get_level_values(0):
                if value != current_year:
                    score_card.loc[index, str(value) + ' Total'] = \
                        modality_group[value]
                else:
                    score_card.loc[index, str(value) + ' YTD'] = \
                        modality_group[value]
            modality_ytd = modality[
                ['TOTALGECOST', 'Year of service_close_date',
                 'Week Number']].loc[
                (
                        modality[
                            'Year of service_close_date'] == current_year - 1) & (
                        modality['Week Number'] <= week_number)]
            modality_ytd_group = \
                modality_ytd.groupby('Year of service_close_date')[
                    'TOTALGECOST'].sum()
            if len(modality_ytd_group) > 0:
                score_card.loc[index, str(
                    modality_ytd_group.index.get_level_values(0)[0]) + ' YTD'] = \
                    modality_ytd_group[
                        modality_ytd_group.index.get_level_values(0)[0]]
        elif row['Business'] != 'ALL' and row['Modality'] == 'ALL' and row[
            'Product Quality Scorecard'] == 'LCS':
            modality = \
                data[['SC Business Segment', 'SC Modality', 'TOTALGECOST',
                      'Year of service_close_date',
                      'Week Number']].loc[
                    (data['SC Business Segment'] == row['Business'])
                    & (data['SC Modality'] != 'ULTRASOUND')
                    & (data['SC Modality'] != 'EXCLUDED')
                    & (data['SC Modality'] != 'OTHER')]
            modality_group = modality.groupby('Year of service_close_date')[
                'TOTALGECOST'].sum()
            for value in modality_group.index.get_level_values(0):
                if value != current_year:
                    score_card.loc[index, str(value) + ' Total'] = \
                        modality_group[value]
                else:
                    score_card.loc[index, str(value) + ' YTD'] = \
                        modality_group[value]
            modality_ytd = modality[
                ['TOTALGECOST', 'Year of service_close_date',
                 'Week Number']].loc[
                (
                        modality[
                            'Year of service_close_date'] == current_year - 1) & (
                        modality['Week Number'] <= week_number)]
            modality_ytd_group = \
                modality_ytd.groupby('Year of service_close_date')[
                    'TOTALGECOST'].sum()
            if len(modality_ytd_group) > 0:
                score_card.loc[index, str(
                    modality_ytd_group.index.get_level_values(0)[0]) + ' YTD'] = \
                    modality_ytd_group[
                        modality_ytd_group.index.get_level_values(0)[0]]
        elif row['Business'] != 'ALL' and row['Modality'] != 'ALL' and row[
            'Product Segment'] == 'ALL':
            prod_seg = \
                data[['SC Business Segment', 'SC Modality',
                      'SC Product Segment', 'TOTALGECOST',
                      'Year of service_close_date', 'Week Number']].loc[
                    (data['SC Business Segment'] == row['Business']) & (
                            data['SC Modality'] == row['Modality']) & (
                            data['SC Product Segment'] == row[
                        'Product Quality Scorecard'])]
            prod_seg_group = prod_seg.groupby('Year of service_close_date')[
                'TOTALGECOST'].sum()
            for value in prod_seg_group.index.get_level_values(0):
                if value != current_year:
                    score_card.loc[index, str(value) + ' Total'] = \
                        prod_seg_group[value]
                else:
                    score_card.loc[index, str(value) + ' YTD'] = \
                        prod_seg_group[value]
            try:
                prod_seg_ytd = prod_seg[
                    ['TOTALGECOST', 'Year of service_close_date',
                     'Week Number']].loc[
                    (prod_seg[
                         'Year of service_close_date'] == current_year - 1) & (
                            prod_seg['Week Number'] <= week_number)]

                prod_seg_ytd_group = \
                    prod_seg_ytd.groupby('Year of service_close_date')[
                        'TOTALGECOST'].sum()

                score_card.loc[index, str(
                    prod_seg_ytd_group.index.get_level_values(0)[
                        0]) + ' YTD'] = \
                    prod_seg_ytd_group[
                        prod_seg_ytd_group.index.get_level_values(0)[0]]
            except KeyError:
                score_card.loc[index, str(current_year - 1) + ' YTD'] = ''
            except IndexError:
                score_card.loc[index, str(current_year - 1) + ' YTD'] = ''
        else:
            product = \
                data[['SC Business Segment', 'SC Modality',
                      'SC Product Segment', 'TOTALGECOST',
                      'Year of service_close_date', 'Week Number']].loc[
                    (data['SC Business Segment'] == row['Business']) & (
                            data['SC Modality'] == row['Modality']) & (
                            data['SC Product Segment'] == row[
                        'Product Segment']) & (
                            data['SC Product'] == row[
                        'Product Quality Scorecard'])]
            product_group = product.groupby('Year of service_close_date')[
                'TOTALGECOST'].sum()
            for value in product_group.index.get_level_values(0):
                if value != current_year:
                    score_card.loc[index, str(value) + ' Total'] = \
                        product_group[value]
                else:
                    score_card.loc[index, str(value) + ' YTD'] = product_group[
                        value]
            try:
                product_ytd = product[
                    ['TOTALGECOST', 'Year of service_close_date',
                     'Week Number']].loc[
                    (product[
                         'Year of service_close_date'] == current_year - 1) & (
                            product['Week Number'] <= week_number)]

                product_ytd_group = \
                    product_ytd.groupby('Year of service_close_date')[
                        'TOTALGECOST'].sum()

                score_card.loc[index, str(
                    product_ytd_group.index.get_level_values(0)[0]) + ' YTD'] = \
                    product_ytd_group[
                        product_ytd_group.index.get_level_values(0)[0]]
            except KeyError:
                score_card.loc[index, str(current_year - 1) + ' YTD'] = ''
            except IndexError:
                score_card.loc[index, str(current_year - 1) + ' YTD'] = ''

    return score_card


def calculate_fmi(score_card, data):
    current_year = datetime.datetime.now().year
    today = datetime.date.today()
    weekday = today.weekday()
    start_delta = datetime.timedelta(days=weekday, weeks=1)
    start_of_week = today - start_delta
    week_number = start_of_week.isocalendar()[1]
    for index, row in score_card.iterrows():
        if row['Product Quality Scorecard'] == 'GEHC':
            gehc = data.groupby('Year')['Total Cost'].sum()
            for value in gehc.index.get_level_values(0):
                if int(value) != current_year:
                    score_card.loc[index, str(value) + ' Total'] = gehc[value]
                else:
                    score_card.loc[index, str(value) + ' YTD'] = gehc[value]
            gehc_ytd = data[['Total Cost', 'Year', 'Week Number']].loc[
                (data['Year'] == current_year - 1) & (
                        data['Week Number'] <= week_number)]
            gehc_ytd_group = gehc_ytd.groupby('Year')['Total Cost'].sum()
            if len(gehc_ytd_group.index.get_level_values(0)) > 0:
                score_card.loc[index, str(
                    gehc_ytd_group.index.get_level_values(0)[0]) + ' YTD'] = \
                    gehc_ytd_group[gehc_ytd_group.index.get_level_values(0)[0]]

        elif row['Product Quality Scorecard'] != 'GEHC' and row[
            'Business'] == 'ALL' and row['Modality'] == 'ALL':
            business = data.loc[data['SC Business Segment'] == row[
                'Product Quality Scorecard']]
            business_group = business.groupby('Year')['Total Cost'].sum()
            for value in business_group.index.get_level_values(0):
                if value != current_year:
                    score_card.loc[index, str(value) + ' Total'] = \
                        business_group[value]
                else:
                    score_card.loc[index, str(value) + ' YTD'] = \
                        business_group[value]
            business_ytd = business[['Total Cost', 'Year', 'Week Number']].loc[
                (business['Year'] == current_year - 1) & (
                        business['Week Number'] <= week_number)]
            business_ytd_group = business_ytd.groupby('Year')[
                'Total Cost'].sum()
            if len(business_ytd_group.index.get_level_values(0)) > 0:
                score_card.loc[index, str(
                    business_ytd_group.index.get_level_values(0)[0]) + ' YTD'] = \
                    business_ytd_group[
                        business_ytd_group.index.get_level_values(0)[0]]
        elif row['Business'] != 'ALL' and row['Modality'] == 'ALL' and row[
            'Product Quality Scorecard'] != 'LCS':
            modality = data[
                ['SC Business Segment', 'SC Modality', 'Total Cost', 'Year',
                 'Week Number']].loc[
                (data['SC Business Segment'] == row['Business']) & (
                        data['SC Modality'] == row['Product Quality Scorecard'])]
            modality_group = modality.groupby('Year')['Total Cost'].sum()
            for value in modality_group.index.get_level_values(0):
                if value != current_year:
                    score_card.loc[index, str(value) + ' Total'] = \
                        modality_group[value]
                else:
                    score_card.loc[index, str(value) + ' YTD'] = \
                        modality_group[value]
            modality_ytd = modality[['Total Cost', 'Year', 'Week Number']].loc[
                (modality['Year'] == current_year - 1) & (
                        modality['Week Number'] <= week_number)]
            modality_ytd_group = modality_ytd.groupby('Year')[
                'Total Cost'].sum()
            if modality_ytd_group.size > 0:
                score_card.loc[index, str(
                    modality_ytd_group.index.get_level_values(0)[
                        0]) + ' YTD'] = \
                    modality_ytd_group[
                        modality_ytd_group.index.get_level_values(0)[0]]
        elif row['Business'] != 'ALL' and row['Modality'] == 'ALL' and row[
            'Product Quality Scorecard'] == 'LCS':
            modality = data[
                ['SC Business Segment', 'SC Modality', 'Total Cost', 'Year',
                 'Week Number']].loc[
                (data['SC Business Segment'] == row['Business'])
                & (data['SC Modality'] != 'ULTRASOUND')
                & (data['SC Modality'] != 'OTHER')
                & (data['SC Modality'] != 'EXCLUDED')]
            modality_group = modality.groupby('Year')['Total Cost'].sum()
            for value in modality_group.index.get_level_values(0):
                if value != current_year:
                    score_card.loc[index, str(value) + ' Total'] = \
                        modality_group[value]
                else:
                    score_card.loc[index, str(value) + ' YTD'] = \
                        modality_group[value]
            modality_ytd = modality[['Total Cost', 'Year', 'Week Number']].loc[
                (modality['Year'] == current_year - 1) & (
                        modality['Week Number'] <= week_number)]
            modality_ytd_group = modality_ytd.groupby('Year')[
                'Total Cost'].sum()
            if modality_ytd_group.size > 0:
                score_card.loc[index, str(
                    modality_ytd_group.index.get_level_values(0)[
                        0]) + ' YTD'] = \
                    modality_ytd_group[
                        modality_ytd_group.index.get_level_values(0)[0]]
        elif row['Business'] != 'ALL' and row['Modality'] != 'ALL' and row[
            'Product Segment'] == 'ALL':
            prod_seg = \
                data[['SC Business Segment', 'SC Modality',
                      'SC Product Segment', 'Total Cost', 'Year',
                      'Week Number']].loc[
                    (data['SC Business Segment'] == row['Business']) & (
                            data['SC Modality'] == row['Modality']) & (
                            data['SC Product Segment'] == row[
                        'Product Quality Scorecard'])]
            prod_seg_group = prod_seg.groupby('Year')['Total Cost'].sum()
            for value in prod_seg_group.index.get_level_values(0):
                if value != current_year:
                    score_card.loc[index, str(value) + ' Total'] = \
                        prod_seg_group[value]
                else:
                    score_card.loc[index, str(value) + ' YTD'] = \
                        prod_seg_group[value]
            try:
                prod_seg_ytd = prod_seg[['Total Cost', 'Year', 'Week Number']].loc[
                    (prod_seg['Year'] == current_year - 1) & (
                            prod_seg['Week Number'] <= week_number)]

                prod_seg_ytd_group = prod_seg_ytd.groupby('Year')[
                    'Total Cost'].sum()

                score_card.loc[index, str(
                    prod_seg_ytd_group.index.get_level_values(0)[
                        0]) + ' YTD'] = \
                    prod_seg_ytd_group[
                        prod_seg_ytd_group.index.get_level_values(0)[0]]
            except KeyError:
                score_card.loc[index, str(current_year - 1) + ' YTD'] = ''
            except IndexError:
                score_card.loc[index, str(current_year - 1) + ' YTD'] = ''
        else:
            product = \
                data[['SC Business Segment', 'SC Modality',
                      'SC Product Segment', 'Total Cost', 'Year',
                      'Week Number']].loc[
                    (data['SC Business Segment'] == row['Business']) & (
                            data['SC Modality'] == row['Modality']) & (
                            data['SC Product Segment'] == row['Product Segment']) &
                    (data['SC Product'] == row['Product Quality Scorecard'])]
            product_group = product.groupby('Year')['Total Cost'].sum()
            for value in product_group.index.get_level_values(0):
                if value != current_year:
                    score_card.loc[index, str(value) + ' Total'] = \
                        product_group[value]
                else:
                    score_card.loc[index, str(value) + ' YTD'] = product_group[
                        value]
            try:
                product_ytd = product[['Total Cost', 'Year', 'Week Number']].loc[
                    (product['Year'] == current_year - 1) & (
                            product['Week Number'] <= week_number)]

                product_ytd_group = product_ytd.groupby('Year')[
                    'Total Cost'].sum()

                score_card.loc[index, str(
                    product_ytd_group.index.get_level_values(0)[0]) + ' YTD'] = \
                    product_ytd_group[
                        product_ytd_group.index.get_level_values(0)[0]]
            except KeyError:
                score_card.loc[index, str(current_year - 1) + ' YTD'] = ''
            except IndexError:
                score_card.loc[index, str(current_year - 1) + ' YTD'] = ''
    return score_card


def calculate_spending(score_card, data):
    current_year = datetime.datetime.now().year
    for index, row in score_card.iterrows():

        if row['Product Quality Scorecard'] == 'GEHC':
            gehc = data.groupby('Year')['Total GE Cost'].sum()

            for value in gehc.index.get_level_values(0):
                if int(value) != current_year:
                    score_card.loc[index, str(value) + ' Total'] = gehc[value]
                else:
                    score_card.loc[index, str(value) + ' YTD'] = gehc[value]
            gehc_ytd = data[['Total GE Cost', 'Year', 'Month']].loc[
                (data['Year'] == current_year - 1)]
            gehc_ytd_group = gehc_ytd.groupby('Year')['Total GE Cost'].sum()
            if gehc_ytd_group.size > 0:
                score_card.loc[index, str(
                    gehc_ytd_group.index.get_level_values(0)[0]) + ' YTD'] = \
                    gehc_ytd_group[
                        gehc_ytd_group.index.get_level_values(0)[0]]

        elif row['Product Quality Scorecard'] != 'GEHC' and row[
            'Business'] == 'ALL' and row['Modality'] == 'ALL':
            business = data.loc[data['SC Business Segment'] == row[
                'Product Quality Scorecard']]
            business_group = business.groupby('Year')['Total GE Cost'].sum()
            for value in business_group.index.get_level_values(0):
                if int(value) != current_year:
                    score_card.loc[index, str(value) + ' Total'] = \
                        business_group[value]
                else:
                    score_card.loc[index, str(value) + ' YTD'] = \
                        business_group[value]
            business_ytd = business[['Total GE Cost', 'Year', 'Month']].loc[
                (business['Year'] == current_year - 1)]
            business_ytd_group = business_ytd.groupby('Year')[
                'Total GE Cost'].sum()
            if business_ytd_group.size > 0:
                score_card.loc[index, str(
                    business_ytd_group.index.get_level_values(0)[
                        0]) + ' YTD'] = \
                    business_ytd_group[
                        business_ytd_group.index.get_level_values(0)[0]]
        elif row['Business'] != 'ALL' and row['Modality'] == 'ALL' and row[
            'Product Quality Scorecard'] != 'LCS':
            modality = \
                data[['SC Business Segment', 'SC Modality', 'Total GE Cost',
                      'Year',
                      'Month']].loc[
                    (data['SC Business Segment'] == row['Business']) & (
                            data['SC Modality'] == row[
                        'Product Quality Scorecard'])]
            modality_group = modality.groupby('Year')['Total GE Cost'].sum()
            for value in modality_group.index.get_level_values(0):
                if int(value) != current_year:
                    score_card.loc[index, str(value) + ' Total'] = \
                        modality_group[value]
                else:
                    score_card.loc[index, str(value) + ' YTD'] = \
                        modality_group[value]
            modality_ytd = modality[['Total GE Cost', 'Year', 'Month']].loc[
                (modality['Year'] == current_year - 1)]
            modality_ytd_group = modality_ytd.groupby('Year')[
                'Total GE Cost'].sum()
            if modality_ytd_group.size > 0:
                score_card.loc[index, str(
                    modality_ytd_group.index.get_level_values(0)[
                        0]) + ' YTD'] = \
                    modality_ytd_group[
                        modality_ytd_group.index.get_level_values(0)[0]]
        elif row['Business'] != 'ALL' and row['Modality'] == 'ALL' and row[
            'Product Quality Scorecard'] == 'LCS':
            modality = \
                data[['SC Business Segment', 'SC Modality', 'Total GE Cost',
                      'Year',
                      'Month']].loc[
                    (data['SC Business Segment'] == row['Business'])
                    & (data['SC Modality'] != 'ULTRASOUND')
                    & (data['SC Modality'] != 'OTHER')
                    & (data['SC Modality'] != 'EXCLUDED')]
            modality_group = modality.groupby('Year')['Total GE Cost'].sum()
            for value in modality_group.index.get_level_values(0):
                if int(value) != current_year:
                    score_card.loc[index, str(value) + ' Total'] = \
                        modality_group[value]
                else:
                    score_card.loc[index, str(value) + ' YTD'] = \
                        modality_group[value]
            modality_ytd = modality[['Total GE Cost', 'Year', 'Month']].loc[
                (modality['Year'] == current_year - 1)]
            modality_ytd_group = modality_ytd.groupby('Year')[
                'Total GE Cost'].sum()
            if modality_ytd_group.size > 0:
                score_card.loc[index, str(
                    modality_ytd_group.index.get_level_values(0)[
                        0]) + ' YTD'] = \
                    modality_ytd_group[
                        modality_ytd_group.index.get_level_values(0)[0]]
        elif row['Business'] != 'ALL' and row['Modality'] != 'ALL' and row[
            'Product Segment'] == 'ALL':
            prod_seg = \
                data[['SC Business Segment', 'SC Modality',
                      'SC Product Segment', 'Total GE Cost',
                      'Year', 'Month']].loc[
                    (data['SC Business Segment'] == row['Business']) & (
                            data['SC Modality'] == row['Modality']) & (
                            data['SC Product Segment'] == row[
                        'Product Quality Scorecard'])]
            prod_seg_group = prod_seg.groupby('Year')['Total GE Cost'].sum()
            for value in prod_seg_group.index.get_level_values(0):
                if int(value) != current_year:
                    score_card.loc[index, str(value) + ' Total'] = \
                        prod_seg_group[value]
                else:

                    score_card.loc[index, str(value) + ' YTD'] = \
                        prod_seg_group[value]
            try:
                prod_seg_ytd = \
                    prod_seg[['Total GE Cost', 'Year', 'Month']].loc[
                        (prod_seg['Year'] == current_year - 1)]

                prod_seg_ytd_group = prod_seg_ytd.groupby('Year')[
                    'Total GE Cost'].sum()

                if prod_seg_ytd_group.size > 0:
                    score_card.loc[index, str(
                        prod_seg_ytd_group.index.get_level_values(0)[
                            0]) + ' YTD'] = \
                        prod_seg_ytd_group[
                            prod_seg_ytd_group.index.get_level_values(0)[0]]
            except KeyError:
                score_card.loc[index, str(current_year - 1) + ' YTD'] = ''
            except IndexError:
                score_card.loc[index, str(current_year - 1) + ' YTD'] = ''
        else:
            product = \
                data[['SC Business Segment', 'SC Modality',
                      'SC Product Segment', 'Total GE Cost',
                      'Year', 'Month']].loc[
                    (data['SC Business Segment'] == row['Business']) & (
                            data['SC Modality'] == row['Modality']) &
                    (data['SC Product Segment'] == row['Product Segment']) &
                    (data['SC Product'] == row['Product Quality Scorecard'])]
            product_group = product.groupby('Year')['Total GE Cost'].sum()
            for value in product_group.index.get_level_values(0):
                if int(value) != current_year:
                    score_card.loc[index, str(value) + ' Total'] = \
                        product_group[value]
                else:

                    score_card.loc[index, str(value) + ' YTD'] = product_group[
                        value]
            try:
                product_ytd = product[['Total GE Cost', 'Year', 'Month']].loc[
                    (product['Year'] == current_year - 1)]

                product_ytd_group = product_ytd.groupby('Year')[
                    'Total GE Cost'].sum()

                if product_ytd_group.size > 0:
                    score_card.loc[index, str(
                        product_ytd_group.index.get_level_values(0)[
                            0]) + ' YTD'] = \
                        product_ytd_group[
                            product_ytd_group.index.get_level_values(0)[0]]
            except KeyError:
                score_card.loc[index, str(current_year - 1) + ' YTD'] = ''
            except IndexError:
                score_card.loc[index, str(current_year - 1) + ' YTD'] = ''
    return score_card


def setup_ydpu_non_mr_structure(data, business_config):
    time_periods = data['Period'].unique()
    for time_period in time_periods:
        data.loc[data['Period'] == time_period, 'Month'] = \
            time_period.split('M')[1]
        data.loc[data['Period'] == time_period, 'Year'] = \
            time_period.split('M')[0]
    data['Month'] = data['Month'].astype(int)
    months = data['Month'].unique()
    for month in months:
        if month % 3 > 0:
            data.loc[data['Month'] == month, "Quarter"] = 'Q' + str(month // 3 + 1)
        else:
            data.loc[data['Month'] == month, "Quarter"] = 'Q' + str(month // 3)

    data['Year'] = data['Year'].astype(int)
    data['SC Modality'] = data['Modality']
    data['SC Product'] = data['Product']
    data['SC Product Segment'] = data['Product Segment']
    modalities = data['Modality'].unique()
    for modality in modalities:
        business_data = business_config.loc[
            business_config['Modality'] == modality]
        if len(business_data) > 0:
            data.loc[data['Modality'] == modality, 'SC Business Segment'] = \
                business_data['Business for scorecard'] \
                    .values[0]
    data = data.loc[
        ((data['Year'] == 2018) & ((data['Quarter'] == 'Q3') | (data['Quarter'] == 'Q4'))) |
        (data['Year'] == 2019)]
    data = data.loc[((data['SC Business Segment'] != '') & (data['SC Modality'] != '') &
                     (data['SC Product Segment'] != '') & (data['SC Product'] != ''))]
    data = data.loc[((data['SC Product Segment'] != '*') & (data['SC Product'] != '*'))]
    return data


def setup_ifr_hierarchy(ifr, sii, ifr_modalities):
    abbr_to_num = {name: num for num, name in enumerate(calendar.month_name) if
                   num}
    months = ifr['Month Name'].unique()
    for month in months:
        ifr.loc[ifr['Month Name'] == month, 'Month'] = abbr_to_num[month]
    ifr['Year'] = ifr['Year'].astype(int)
    ifr['Month'] = ifr['Month'].astype(int)
    ifr['Modality_Code'].fillna('', inplace=True)
    ifr['Business_ segment_code'].fillna('', inplace=True)
    ifr['Sub Family Code (or) Product group code'].fillna('', inplace=True)
    ifr['lookup'] = ifr['Modality_Code'] + ifr['Business_ segment_code'] + ifr[
        'Sub Family Code (or) Product group code']
    if ifr['Measured Value'].dtype == np.object:
        ifr['Measured Value'] = ifr['Measured Value'].str.replace(',', '').astype(int)
    else:
        ifr['Measured Value'] = ifr['Measured Value'].astype(int)
    lookup_values = ifr['lookup'].unique()
    for lookup_value in lookup_values:

        sii_data = sii.loc[sii['lookup value'] == lookup_value]

        if sii_data.size > 0:
            ifr.loc[ifr['lookup'] == lookup_value, 'SC Business Segment'] = \
                sii_data['Business'].values[0]
            ifr.loc[ifr['lookup'] == lookup_value, 'SC Modality'] = \
                sii_data['Modality'].values[0]
            ifr.loc[ifr['lookup'] == lookup_value, 'SC Product Segment'] = \
                sii_data['Product Segment'].values[0]
            ifr.loc[ifr['lookup'] == lookup_value, 'SC Product'] = \
                sii_data['sub_family_code (Ultrasound Only) ' \
                         'or product_group_code'].values[0]
        else:
            ifr_modality_values = ifr.loc[ifr['lookup'] == lookup_value][
                'Modality_Code'].unique()
            for ifr_modality_value in ifr_modality_values:
                modality_data = ifr_modalities.loc[
                    ifr_modalities['SII'] == ifr_modality_value]
                if modality_data.size > 0:
                    ifr.loc[(ifr['lookup'] == lookup_value) & (
                            ifr['Modality_Code'] == ifr_modality_value),
                            'SC Business Segment'] = modality_data['Business']
                    ifr.loc[(ifr['lookup'] == lookup_value) & (
                            ifr['Modality_Code'] == ifr_modality_value),
                            'SC Modality'] = modality_data['PQ Modality']
                    ifr.loc[(ifr['lookup'] == lookup_value) & (
                            ifr['Modality_Code'] == ifr_modality_value),
                            'SC Product Segment'] = 'OTHER'

    ifr['SC Business Segment'].fillna('', inplace=True)
    ifr['SC Modality'].fillna('', inplace=True)
    ifr['SC Product Segment'].fillna('', inplace=True)
    ifr['SC Product'].fillna('', inplace=True)
    null_segment_data = ifr.loc[ifr['SC Business Segment'] == '']
    if null_segment_data.size > 0:
        ifr.loc[
            ifr['SC Business Segment'] == '', 'SC Product Segment'] = 'OTHER'
        ifr.loc[ifr['SC Business Segment'] == '', 'SC Modality'] = 'OTHER'
        ifr.loc[ifr[
                    'SC Business Segment'] == '', 'SC Business Segment'] = 'GEHC OTHER'
    null_modality = ifr.loc[ifr['SC Modality'] == '']
    if null_modality.size > 0:
        ifr.loc[ifr['SC Modality'] == '', 'SC Modality'] = 'OTHER'
    null_prod_seg = ifr.loc[ifr['SC Product Segment'] == '']
    if null_prod_seg.size > 0:
        ifr.loc[
            ifr['SC Product Segment'] == '', 'SC Product Segment'] = 'OTHER'
    null_products = ifr.loc[ifr['SC Product'] == '']
    if null_products.size > 0:
        products = ifr.loc[ifr['SC Product'] == ''][
            'Sub Family Code (or) Product group code'].unique()
        for product in products:
            if product == '' or product == 'NAN' or product == 'nan' or product is None:
                product = '<blank in source>'
            ifr.loc[(ifr['SC Product'] == '') & (
                    ifr['Sub Family Code (or) Product group code'] == product),
                    'SC Product'] = product
    ifr.loc[ifr['SC Product'] == '', 'SC Product'] = '<BLANK IN SOURCE>'
    current_year = datetime.datetime.now().year
    current_month = datetime.datetime.now().month
    today = datetime.date.today()
    weekday = today.weekday()
    start_delta = datetime.timedelta(days=weekday, weeks=1)
    start_of_week = today - start_delta
    week_number = start_of_week.isocalendar()[1]
    ifr = ifr.loc[(ifr['Year'] < current_year) |
                  ((ifr['Year'] == current_year) &
                   (ifr['Month'] <= (current_month)) & (ifr['Week Number'] <= (week_number)))]

    return ifr


def calculate_ifr_rate(score_card, data):
    current_year = datetime.datetime.now().year
    today = datetime.date.today()
    weekday = today.weekday()
    start_delta = datetime.timedelta(days=weekday, weeks=1)
    start_of_week = today - start_delta
    week_number = start_of_week.isocalendar()[1]
    for index, row in score_card.iterrows():
        if row['Product Quality Scorecard'] == 'GEHC':
            gehc_sr = data[['Year', 'Measure Names', 'Measured Value']].loc[
                (data['Measure Names'] == 'SR Count') & (
                        data['Year'] > 2015)].groupby('Year')[
                'Measured Value'].sum()

            gehc_ib = (data[['Year', 'Measure Names', 'Measured Value']].loc[
                           (data['Measure Names'] == 'IB Count') & (
                                   data['Year'] > 2015)].groupby('Year')[
                           'Measured Value'].sum())

            ifr_rate = gehc_sr / gehc_ib

            for value in ifr_rate.index.get_level_values(0):
                if int(value) != current_year:
                    score_card.loc[index, str(value) + ' Total'] = ifr_rate[
                        value]
                else:
                    score_card.loc[index, str(value) + ' YTD'] = ifr_rate[
                        value]

            gehc_sr_ytd = \
                data[['Year', 'Week Number', 'Measure Names', 'Measured Value']].loc[
                    (data['Year'] == current_year - 1) & (
                            data['Week Number'] <= week_number)
                    & (data['Measure Names'] == 'SR Count')].groupby('Year')[
                    'Measured Value'].sum()

            gehc_ib_ytd = (
                data[['Year', 'Week Number', 'Measure Names', 'Measured Value']].loc[
                    (data['Year'] == current_year - 1) &
                    (data['Week Number'] <= week_number) &
                    (data['Measure Names'] == 'IB Count')].groupby('Year')[
                    'Measured Value'].sum())

            ifr_rate_ytd = gehc_sr_ytd / gehc_ib_ytd
            try:
                score_card.loc[index, str(
                    ifr_rate_ytd.index.get_level_values(0)[0]) + ' YTD'] = \
                    ifr_rate_ytd[ifr_rate_ytd.index.get_level_values(0)[0]]
            except IndexError:
                score_card.loc[index, str(current_year - 1) + ' YTD'] = ''
        elif row['Product Quality Scorecard'] != 'GEHC' and row[
            'Business'] == 'ALL' and row['Modality'] == 'ALL':
            business = \
                data[['Year', 'Measure Names', 'Week Number', 'Measured Value']].loc[
                    (data['SC Business Segment'] ==
                     row[
                         'Product Quality Scorecard']) & (
                            data['Year'] > 2015)]

            sr_business = \
                business[['Year', 'Measure Names', 'Measured Value']].loc[
                    business['Measure Names'] == 'SR Count'].groupby('Year')[
                    'Measured Value'].sum()

            ib_business = (
                business[['Year', 'Measure Names', 'Measured Value']].loc[
                    business['Measure Names'] == 'IB Count'].groupby('Year')[
                    'Measured Value'].sum())

            ifr_rate_business = sr_business / ib_business

            for value in ifr_rate_business.index.get_level_values(0):
                if int(value) != current_year:
                    score_card.loc[index, str(value) + ' Total'] = \
                        ifr_rate_business[value]
                else:
                    score_card.loc[index, str(value) + ' YTD'] = \
                        ifr_rate_business[value]

            business_sr_ytd = \
                business[
                    ['Year', 'Week Number', 'Measure Names', 'Measured Value']].loc[
                    (business['Year'] == current_year - 1) & (
                            business['Week Number'] <= week_number)
                    & (business['Measure Names'] == 'SR Count')].groupby(
                    'Year')[
                    'Measured Value'].sum()

            business_ib_ytd = (
                business[
                    ['Year', 'Week Number', 'Measure Names', 'Measured Value']].loc[
                    (business['Year'] == current_year - 1) &
                    (business['Week Number'] <= week_number) &
                    (business['Measure Names'] == 'IB Count')].groupby('Year')[
                    'Measured Value'].sum())

            business_ifr_rate_ytd = business_sr_ytd / business_ib_ytd
            try:
                score_card.loc[index, str(
                    business_ifr_rate_ytd.index.get_level_values(0)[
                        0]) + ' YTD'] = \
                    business_ifr_rate_ytd[
                        business_ifr_rate_ytd.index.get_level_values(0)[0]]
            except IndexError:
                score_card.loc[index, str(current_year - 1) + ' YTD'] = ''

        elif row['Business'] != 'ALL' and row['Modality'] == 'ALL' and row[
            'Product Quality Scorecard'] != 'LCS':
            modality = data[
                ['SC Business Segment', 'SC Modality', 'Week Number', 'Year',
                 'Measure Names',
                 'Measured Value']].loc[
                (data['SC Business Segment'] == row['Business'])
                & ((data['SC Modality'] == row['Product Quality Scorecard'])
                   & (data['Year'] > 2015))]

            sr_modality = \
                modality[['Year', 'Measure Names', 'Measured Value']].loc[
                    modality['Measure Names'] == 'SR Count'].groupby('Year')[
                    'Measured Value'].sum()

            ib_modality = (
                modality[['Year', 'Measure Names', 'Measured Value']].loc[
                    modality['Measure Names'] == 'IB Count'].groupby('Year')[
                    'Measured Value'].sum())

            ifr_rate_modality = sr_modality / ib_modality

            for value in ifr_rate_modality.index.get_level_values(0):
                if int(value) != current_year:
                    score_card.loc[index, str(value) + ' Total'] = \
                        ifr_rate_modality[value]
                else:
                    score_card.loc[index, str(value) + ' YTD'] = \
                        ifr_rate_modality[value]

            modality_sr_ytd = \
                modality[
                    ['Year', 'Week Number', 'Measure Names', 'Measured Value']].loc[
                    (modality['Year'] == current_year - 1) & (
                            modality['Week Number'] <= week_number)
                    & (modality['Measure Names'] == 'SR Count')].groupby(
                    'Year')[
                    'Measured Value'].sum()

            modality_ib_ytd = (
                modality[
                    ['Year', 'Week Number', 'Measure Names', 'Measured Value']].loc[
                    (modality['Year'] == current_year - 1) &
                    (modality['Week Number'] <= week_number) &
                    (modality['Measure Names'] == 'IB Count')].groupby('Year')[
                    'Measured Value'].sum())

            modality_ifr_rate_ytd = modality_sr_ytd / modality_ib_ytd
            try:
                score_card.loc[index, str(
                    modality_ifr_rate_ytd.index.get_level_values(0)[
                        0]) + ' YTD'] = \
                    modality_ifr_rate_ytd[
                        modality_ifr_rate_ytd.index.get_level_values(0)[0]]
            except IndexError:
                score_card.loc[index, str(current_year - 1) + ' YTD'] = ''
        elif row['Business'] != 'ALL' and row['Modality'] == 'ALL' and row[
            'Product Quality Scorecard'] == 'LCS':
            modality = data[
                ['SC Business Segment', 'SC Modality', 'Week Number', 'Year',
                 'Measure Names',
                 'Measured Value']].loc[
                (data['SC Business Segment'] == row['Business']) &
                ((data['SC Modality'] != 'ULTRASOUND')
                 & (data['SC Modality'] != 'EXCLUDED')
                 & (data['SC Modality'] != 'OTHER')
                 & (data['Year'] > 2015))]

            sr_modality = \
                modality[['Year', 'Measure Names', 'Measured Value']].loc[
                    modality['Measure Names'] == 'SR Count'].groupby('Year')[
                    'Measured Value'].sum()

            ib_modality = (
                modality[['Year', 'Measure Names', 'Measured Value']].loc[
                    modality['Measure Names'] == 'IB Count'].groupby('Year')[
                    'Measured Value'].sum())

            ifr_rate_modality = sr_modality / ib_modality

            for value in ifr_rate_modality.index.get_level_values(0):
                if int(value) != current_year:
                    score_card.loc[index, str(value) + ' Total'] = \
                        ifr_rate_modality[value]
                else:
                    score_card.loc[index, str(value) + ' YTD'] = \
                        ifr_rate_modality[value]

            modality_sr_ytd = \
                modality[
                    ['Year', 'Week Number', 'Measure Names', 'Measured Value']].loc[
                    (modality['Year'] == current_year - 1) & (
                            modality['Week Number'] <= week_number)
                    & (modality['Measure Names'] == 'SR Count')].groupby(
                    'Year')[
                    'Measured Value'].sum()

            modality_ib_ytd = (
                modality[
                    ['Year', 'Week Number', 'Measure Names', 'Measured Value']].loc[
                    (modality['Year'] == current_year - 1) &
                    (modality['Week Number'] <= week_number) &
                    (modality['Measure Names'] == 'IB Count')].groupby('Year')[
                    'Measured Value'].sum())

            modality_ifr_rate_ytd = modality_sr_ytd / modality_ib_ytd
            try:
                score_card.loc[index, str(
                    modality_ifr_rate_ytd.index.get_level_values(0)[
                        0]) + ' YTD'] = \
                    modality_ifr_rate_ytd[
                        modality_ifr_rate_ytd.index.get_level_values(0)[0]]
            except IndexError:
                score_card.loc[index, str(current_year - 1) + ' YTD'] = ''

        elif row['Business'] != 'ALL' and row['Modality'] != 'ALL' and row[
            'Product Segment'] == 'ALL':
            prod_seg = \
                data[['SC Business Segment', 'SC Modality',
                      'SC Product Segment',
                      'Year', 'Week Number', 'Measure Names', 'Measured Value']].loc[
                    (data['SC Business Segment'] == row['Business']) & (
                            data['SC Modality'] ==
                            row['Modality']) & (
                            data['SC Product Segment'] == row[
                        'Product Quality Scorecard']) & (
                            data['Year'] > 2015)]

            sr_prod_seg = \
                prod_seg[
                    ['Year', 'Week Number', 'Measure Names', 'Measured Value']].loc[
                    prod_seg['Measure Names'] == 'SR Count'].groupby(['Year'])[
                    'Measured Value'].sum()

            ib_prod_seg = (
                prod_seg[['Year', 'Measure Names', 'Measured Value']].loc[
                    prod_seg['Measure Names'] == 'IB Count'].groupby('Year')[
                    'Measured Value'].sum())

            ifr_rate_prod_seg = sr_prod_seg / ib_prod_seg

            for value in ifr_rate_prod_seg.index.get_level_values(0):
                if int(value) != current_year:
                    score_card.loc[index, str(value) + ' Total'] = \
                        ifr_rate_prod_seg[value]
                else:
                    score_card.loc[index, str(value) + ' YTD'] = \
                        ifr_rate_prod_seg[value]

            prod_seg_sr_ytd = \
                prod_seg[
                    ['Year', 'Week Number', 'Measure Names', 'Measured Value']].loc[
                    (prod_seg['Year'] == current_year - 1) & (
                            prod_seg['Week Number'] <= week_number)
                    & (prod_seg['Measure Names'] == 'SR Count')].groupby(
                    'Year')[
                    'Measured Value'].sum()

            prod_seg_ib_ytd = (
                prod_seg[
                    ['Year', 'Week Number', 'Measure Names', 'Measured Value']].loc[
                    (prod_seg['Year'] == current_year - 1) &
                    (prod_seg['Week Number'] <= week_number) &
                    (prod_seg['Measure Names'] == 'IB Count')].groupby('Year')[
                    'Measured Value'].sum())

            prod_seg_ifr_rate_ytd = prod_seg_sr_ytd / prod_seg_ib_ytd
            try:
                score_card.loc[index, str(
                    prod_seg_ifr_rate_ytd.index.get_level_values(0)[
                        0]) + ' YTD'] = \
                    prod_seg_ifr_rate_ytd[
                        prod_seg_ifr_rate_ytd.index.get_level_values(0)[0]]
            except IndexError:
                score_card.loc[index, str(current_year - 1) + ' YTD'] = ''
        else:
            product = \
                data[['SC Business Segment', 'SC Modality',
                      'SC Product Segment',
                      'Year', 'Week Number', 'Measure Names', 'Measured Value']].loc[
                    (data['SC Business Segment'] == row['Business']) & (
                            data['SC Modality'] == row['Modality']) &
                    (data['SC Product Segment'] == row['Product Segment']) &
                    (
                            data['SC Product'] == row[
                        'Product Quality Scorecard']) & (
                            data['Year'] > 2015)]

            sr_product = \
                product[
                    ['Year', 'Week Number', 'Measure Names', 'Measured Value']].loc[
                    product['Measure Names'] == 'SR Count'].groupby(['Year'])[
                    'Measured Value'].sum()

            ib_product = (
                product[['Year', 'Measure Names', 'Measured Value']].loc[
                    product['Measure Names'] == 'IB Count'].groupby('Year')[
                    'Measured Value'].sum())

            ifr_rate_product = sr_product / ib_product

            for value in ifr_rate_product.index.get_level_values(0):
                if int(value) != current_year:
                    score_card.loc[index, str(value) + ' Total'] = \
                        ifr_rate_product[value]
                else:
                    score_card.loc[index, str(value) + ' YTD'] = \
                        ifr_rate_product[value]

            product_sr_ytd = \
                product[
                    ['Year', 'Week Number', 'Measure Names', 'Measured Value']].loc[
                    (product['Year'] == current_year - 1) & (
                            product['Week Number'] <= week_number)
                    & (product['Measure Names'] == 'SR Count')].groupby(
                    'Year')[
                    'Measured Value'].sum()

            product_ib_ytd = (
                product[
                    ['Year', 'Week Number', 'Measure Names', 'Measured Value']].loc[
                    (product['Year'] == current_year - 1) &
                    (product['Week Number'] <= week_number) &
                    (product['Measure Names'] == 'IB Count')].groupby('Year')[
                    'Measured Value'].sum())

            product_ifr_rate_ytd = product_sr_ytd / product_ib_ytd
            try:
                score_card.loc[index, str(
                    product_ifr_rate_ytd.index.get_level_values(0)[
                        0]) + ' YTD'] = \
                    product_ifr_rate_ytd[
                        product_ifr_rate_ytd.index.get_level_values(0)[0]]
            except IndexError:
                score_card.loc[index, str(current_year - 1) + ' YTD'] = ''
    return score_card


def setup_complaint_rate_hierarchy(data, chu_hierarchy):
    data["Measure Values"] = scorecard_functions.remove_comma_from_df_column(data["Measure Values"])
    data['Modality'].fillna('', inplace=True)
    data['Product Group / Modality Segment - IB'].fillna('', inplace=True)
    data['CR Lookup Value'] = data['Modality'] + data[
        'Product Group / Modality Segment - IB']
    time_axes = data['By Time Label'].unique()
    for time in time_axes:
        data.loc[data['By Time Label'] == time, 'Year'] = time.split('M')[0]
        data.loc[data['By Time Label'] == time, 'Month'] = time.split('M')[1]
    data["Year"] = data["Year"].astype(int)
    data["Month"] = data["Month"].astype(int)
    modalities = data['Modality'].unique()
    for modality in modalities:
        modality_data = chu_hierarchy.loc[
            chu_hierarchy['Modality'] == modality]
        if modality_data.size > 0:
            data.loc[data['Modality'] == modality, 'SC Business Segment'] = \
                modality_data['Business for Scorecard'] \
                    .values[0]
            data.loc[data['Modality'] == modality, 'SC Modality'] = \
                modality_data['Modality for Scorecard'] \
                    .values[0]
        else:
            data.loc[data[
                         'Modality'] == modality, 'SC Business Segment'] = 'GEHC OTHER'
            data.loc[data['Modality'] == modality, 'SC Modality'] = 'OTHER'
    cr_lookup_values = data['CR Lookup Value'].unique()
    for cr_lookup_value in cr_lookup_values:
        lookup_data = chu_hierarchy.loc[
            chu_hierarchy['Lookup Value'] == cr_lookup_value]
        if lookup_data.size > 0:
            data.loc[data[
                         'CR Lookup Value'] == cr_lookup_value, 'SC Product Segment'] = \
                lookup_data['Product Segment for Scorecard'].values[0]
        else:
            data.loc[data[
                         'CR Lookup Value'] == cr_lookup_value,
                     'SC Product Segment'] = 'OTHER'
    data['SC Product'] = data['Product Group / Modality Segment - IB']

    data.loc[(data['SC Product'] == 'NAN'), 'SC Product'] = \
        pd.Series('<BLANK IN SOURCE>')
    current_year = datetime.datetime.now().year
    current_month = datetime.datetime.now().month
    today = datetime.date.today()
    weekday = today.weekday()
    start_delta = datetime.timedelta(days=weekday, weeks=1)
    start_of_week = today - start_delta
    week_number = start_of_week.isocalendar()[1]

    data = data.loc[
        (data['Year'] < current_year) | ((data['Year'] == current_year)
                                         & (data['Month'] <= (current_month)) & (
                                                 data['Week Number'] <= (week_number)))]

    return data


def setup_complaint_rate_hierarchy_chu(data, chu_hierarchy):
    data['Modality'].fillna('', inplace=True)
    data['Product Group / Modality Segment - IB'].fillna('', inplace=True)
    data['CR Lookup Value'] = data['Modality'] + data[
        'Product Group / Modality Segment - IB']
    time_axes = data['Quarter'].unique()
    for time in time_axes:
        data.loc[data['Quarter'] == time, 'Year'] = time[-4:]
        data.loc[data['Quarter'] == time, 'Quarter new'] = time[1:2]
    del data["Quarter"]
    data.rename(columns={"Quarter new": "Quarter"}, inplace=True)
    data["Year"] = data["Year"].astype(int)
    data["Quarter"] = data["Quarter"].astype(int)
    modalities = data['Modality'].unique()
    for modality in modalities:
        modality_data = chu_hierarchy.loc[
            chu_hierarchy['Modality'] == modality]
        if modality_data.size > 0:
            data.loc[data['Modality'] == modality, 'SC Business Segment'] = \
                modality_data['Business for Scorecard'] \
                    .values[0]
            data.loc[data['Modality'] == modality, 'SC Modality'] = \
                modality_data['Modality for Scorecard'] \
                    .values[0]
        else:
            data.loc[data[
                         'Modality'] == modality, 'SC Business Segment'] = 'GEHC OTHER'
            data.loc[data['Modality'] == modality, 'SC Modality'] = 'OTHER'
    cr_lookup_values = data['CR Lookup Value'].unique()
    for cr_lookup_value in cr_lookup_values:
        lookup_data = chu_hierarchy.loc[
            chu_hierarchy['Lookup Value'] == cr_lookup_value]
        if lookup_data.size > 0:
            data.loc[data[
                         'CR Lookup Value'] == cr_lookup_value, 'SC Product Segment'] = \
                lookup_data['Product Segment for Scorecard'].values[0]
        else:
            data.loc[data[
                         'CR Lookup Value'] == cr_lookup_value,
                     'SC Product Segment'] = 'OTHER'
    data['SC Product'] = data['Product Group / Modality Segment - IB']

    data.loc[(data['SC Product'] == 'NAN'), 'SC Product'] = \
        pd.Series('<BLANK IN SOURCE>')
    current_year = datetime.datetime.now().year
    current_month = datetime.datetime.now().month
    if current_month % 2 == 0:
        current_quarter = current_month / 3
    else:
        current_quarter = current_month / 3 + 1
    data = data.loc[
        (data['Year'] < current_year) | ((data['Year'] == current_year)
                                         & (data['Quarter'] <= (current_quarter)))]
    return data


def apply_transformation(transformation, data):
    """
    Look up on transformation data to standardize names
    :param transformation: transformation lookup
    :param data: souece data that needs to be transformed
    :return: updated data (data frame)
    """
    for index, row in transformation.iterrows():
        data.replace(to_replace=row['Product Quality Scorecard'],
                     value=row['TRANSFORMATION'], inplace=True)
    return data


def to_upper(data, columns):
    """
    Converts data frame column to upper case
    :param data: input data frame
    :param column: name of the column whose contents need to be converted to
    upper case (String)
    :return: data frame column with upper case conversion
    """
    for column in columns:
        data[column] = data[column].apply(lambda x: str(x).upper())
    return data


def calculate_oti_q4(score_card, data):
    """
        Calculate On Time Install for scorecard with Q4 value for the previous
        year
        :param score_card: scorecard structure for the metric (data frame)
        :param data: source data after hierarchy look up (data frame)
        :return: scorecard data
        """
    current_year = datetime.datetime.now().year
    for index, row in score_card.iterrows():
        if row['Product Quality Scorecard'] == 'GEHC':
            gehc_on_time = \
                data[['Year', 'Measure Names', 'Measure Values']].loc[
                    data['Measure Names'] == '# of On Time Install'].groupby(
                    'Year')['Measure Values'].sum()
            gehc_overall = data[['Year', 'Measure Names', 'Measure Values']] \
                .groupby('Year')['Measure Values'].sum()
            gehc_oti = gehc_on_time / gehc_overall * 100
            for value in gehc_oti.index.get_level_values(0):
                if int(value) != current_year:
                    score_card.loc[index, str(value) + ' Total'] = gehc_oti[
                        value]
                else:
                    score_card.loc[index, str(value) + ' YTD'] = gehc_oti[
                        value]

            gehc_on_time_ytd = \
                data[['Year', 'Month', 'Measure Names', 'Measure Values']].loc[
                    (data['Year'] == current_year - 1) & (
                            data['Month'] >= 10)
                    & (
                            data[
                                'Measure Names'] == '# of On Time Install')].groupby(
                    'Year')['Measure Values'].sum()
            gehc_overall_ytd = \
                data[['Year', 'Month', 'Measure Names', 'Measure Values']].loc[
                    (data['Year'] == current_year - 1) & (
                            data['Month'] >= 10)] \
                    .groupby('Year')['Measure Values'].sum()
            gehc_oti_ytd = gehc_on_time_ytd / gehc_overall_ytd * 100

            try:
                score_card.loc[index, "Q4 " + str(
                    gehc_oti_ytd.index.get_level_values(0)[0])[-2:]] = \
                    gehc_oti_ytd[gehc_oti_ytd.index.get_level_values(0)[0]]
            except IndexError:
                score_card.loc[index, "Q4 " + str(current_year - 1)[-2:]] = ''
        elif row['Product Quality Scorecard'] != 'GEHC' and row[
            'Business'] == 'ALL' and row['Modality'] == 'ALL':
            business = data.loc[data['SC Business Segment'] == row[
                'Product Quality Scorecard']]

            business_on_time = \
                business[['Year', 'Measure Names', 'Measure Values']].loc[
                    business[
                        'Measure Names'] == '# of On Time Install'].groupby(
                    'Year')['Measure Values'].sum()
            business_overall = \
                business[['Year', 'Measure Names', 'Measure Values']] \
                    .groupby('Year')['Measure Values'].sum()
            business_oti = business_on_time / business_overall * 100
            for value in business_oti.index.get_level_values(0):
                if int(value) != current_year:
                    score_card.loc[index, str(value) + ' Total'] = \
                        business_oti[value]
                else:
                    score_card.loc[index, str(value) + ' YTD'] = business_oti[
                        value]

            business_on_time_ytd = \
                business[
                    ['Year', 'Measure Names', 'Measure Values', 'Month']].loc[
                    (business['Year'] == current_year - 1) & (
                            business['Month'] >= 10)
                    & (
                            business[
                                'Measure Names'] == '# of On Time Install')].groupby(
                    'Year')['Measure Values'].sum()

            business_overall_ytd = \
                business[
                    ['Year', 'Measure Names', 'Measure Values', 'Month']].loc[
                    (business['Year'] == current_year - 1) & (
                            business['Month'] >= 10)] \
                    .groupby('Year')['Measure Values'].sum()

            business_oti_ytd = business_on_time_ytd / business_overall_ytd * 100

            try:
                score_card.loc[index, "Q4 " + str(
                    business_oti_ytd.index.get_level_values(0)[0])[-2:]] = \
                    business_oti_ytd[
                        business_oti_ytd.index.get_level_values(0)[0]]

            except IndexError:
                score_card.loc[index, "Q4 " + str(current_year - 1)[-2:]] = ''

        elif row['Business'] != 'ALL' and row['Modality'] == 'ALL' and row[
            'Product Quality Scorecard'] != 'LCS':
            modality = data[
                ['SC Business Segment', 'SC Modality', 'Month', 'Year',
                 'Measure Names',
                 'Measure Values']].loc[
                (data['SC Business Segment'] == row['Business']) & (
                        data['SC Modality'] == row['Product Quality Scorecard'])]

            modality_on_time = \
                modality[['Year', 'Measure Names', 'Measure Values']].loc[
                    modality[
                        'Measure Names'] == '# of On Time Install'].groupby(
                    'Year')['Measure Values'].sum()

            modality_overall = \
                modality[['Year', 'Measure Names', 'Measure Values']]. \
                    groupby('Year')['Measure Values'].sum()

            modality_oti = modality_on_time / modality_overall * 100

            for value in modality_oti.index.get_level_values(0):
                if int(value) != current_year:
                    score_card.loc[index, str(value) + ' Total'] = \
                        modality_oti[value]
                else:
                    score_card.loc[index, str(value) + ' YTD'] = modality_oti[
                        value]

            modality_on_time_ytd = \
                modality[
                    ['Year', 'Month', 'Measure Names', 'Measure Values']].loc[
                    (modality['Year'] == current_year - 1) & (
                            modality['Month'] >= 10)
                    & (
                            modality[
                                'Measure Names'] == '# of On Time Install')].groupby(
                    'Year')['Measure Values'].sum()

            modality_overall_ytd = \
                modality[
                    ['Year', 'Month', 'Measure Names', 'Measure Values']].loc[
                    (modality['Year'] == current_year - 1) & (
                            modality['Month'] >= 10)] \
                    .groupby('Year')['Measure Values'].sum()

            modality_oti_ytd = modality_on_time_ytd / modality_overall_ytd * 100

            try:
                score_card.loc[index, "Q4 " + str(
                    modality_oti_ytd.index.get_level_values(0)[0])[-2:]] = \
                    modality_oti_ytd[
                        modality_oti_ytd.index.get_level_values(0)[0]]
            except IndexError:
                score_card.loc[index, "Q4 " + str(current_year - 1)[-2:]] = ''
        elif row['Business'] != 'ALL' and row['Modality'] == 'ALL' and row[
            'Product Quality Scorecard'] == 'LCS':
            modality = data[
                ['SC Business Segment', 'SC Modality', 'Month', 'Year',
                 'Measure Names',
                 'Measure Values']].loc[
                (data['SC Business Segment'] == row['Business'])
                & (data['SC Modality'] != 'ULTRASOUND')
                & (data['SC Modality'] != 'OTHER')
                & (data['SC Modality'] != 'EXCLUDED')]

            modality_on_time = \
                modality[['Year', 'Measure Names', 'Measure Values']].loc[
                    modality[
                        'Measure Names'] == '# of On Time Install'].groupby(
                    'Year')['Measure Values'].sum()

            modality_overall = \
                modality[['Year', 'Measure Names', 'Measure Values']]. \
                    groupby('Year')['Measure Values'].sum()

            modality_oti = modality_on_time / modality_overall * 100

            for value in modality_oti.index.get_level_values(0):
                if int(value) != current_year:
                    score_card.loc[index, str(value) + ' Total'] = \
                        modality_oti[value]
                else:
                    score_card.loc[index, str(value) + ' YTD'] = modality_oti[
                        value]

            modality_on_time_ytd = \
                modality[
                    ['Year', 'Month', 'Measure Names', 'Measure Values']].loc[
                    (modality['Year'] == current_year - 1) & (
                            modality['Month'] >= 10)
                    & (
                            modality[
                                'Measure Names'] == '# of On Time Install')].groupby(
                    'Year')['Measure Values'].sum()

            modality_overall_ytd = \
                modality[
                    ['Year', 'Month', 'Measure Names', 'Measure Values']].loc[
                    (modality['Year'] == current_year - 1) & (
                            modality['Month'] >= 10)] \
                    .groupby('Year')['Measure Values'].sum()

            modality_oti_ytd = modality_on_time_ytd / modality_overall_ytd * 100

            try:
                score_card.loc[index, "Q4 " + str(
                    modality_oti_ytd.index.get_level_values(0)[0])[-2:]] = \
                    modality_oti_ytd[
                        modality_oti_ytd.index.get_level_values(0)[0]]
            except IndexError:
                score_card.loc[index, "Q4 " + str(current_year - 1)[-2:]] = ''

        elif row['Business'] != 'ALL' and row['Modality'] != 'ALL' and row[
            'Product Segment'] == 'ALL':
            prod_seg = \
                data[['SC Business Segment', 'SC Modality',
                      'SC Product Segment',
                      'Year', 'Measure Names', 'Measure Values', 'Month']].loc[
                    (data['SC Business Segment'] == row['Business']) & (
                            data['SC Modality'] ==
                            row['Modality']) & (
                            data['SC Product Segment'] == row[
                        'Product Quality Scorecard'])]

            prod_seg_on_time = \
                prod_seg[['Year', 'Measure Names', 'Measure Values']].loc[
                    prod_seg[
                        'Measure Names'] == '# of On Time Install'].groupby(
                    'Year')['Measure Values'].sum()

            prod_seg_overall = \
                prod_seg[['Year', 'Measure Names', 'Measure Values']]. \
                    groupby('Year')['Measure Values'].sum()

            prod_seg_oti = prod_seg_on_time / prod_seg_overall * 100

            for value in prod_seg_oti.index.get_level_values(0):
                if int(value) != current_year:
                    score_card.loc[index, str(value) + ' Total'] = \
                        prod_seg_oti[value]
                else:
                    score_card.loc[index, str(value) + ' YTD'] = prod_seg_oti[
                        value]

            prod_seg_on_time_ytd = \
                prod_seg[
                    ['Year', 'Month', 'Measure Names', 'Measure Values']].loc[
                    (prod_seg['Year'] == current_year - 1) & (
                            prod_seg['Month'] >= 10)
                    & (prod_seg['Measure Names'] == '# of On Time Install')] \
                    .groupby('Year')['Measure Values'].sum()

            prod_seg_overall_ytd = \
                prod_seg[
                    ['Year', 'Month', 'Measure Names', 'Measure Values']].loc[
                    (prod_seg['Year'] == current_year - 1) & (
                            prod_seg['Month'] >= 10)] \
                    .groupby('Year')['Measure Values'].sum()

            prod_seg_oti = prod_seg_on_time_ytd / prod_seg_overall_ytd * 100

            try:
                score_card.loc[index, "Q4 " + str(
                    prod_seg_oti.index.get_level_values(0)[0])[-2:]] = \
                    prod_seg_oti[prod_seg_oti.index.get_level_values(0)[0]]
            except IndexError:
                score_card.loc[index, "Q4 " + str(current_year - 1)[-2:]] = ''

        else:
            product = \
                data[['SC Business Segment', 'SC Modality',
                      'SC Product Segment', 'SC Product',
                      'Year', 'Measure Names', 'Measure Values', 'Month']].loc[
                    (data['SC Business Segment'] == row['Business']) & (
                            data['SC Modality'] ==
                            row['Modality']) & (
                            data['SC Product Segment'] == row['Product Segment']) &
                    (data['SC Product'] == row['Product Quality Scorecard'])]

            product_on_time = \
                product[['Year', 'Measure Names', 'Measure Values']].loc[
                    product[
                        'Measure Names'] == '# of On Time Install'].groupby(
                    'Year')['Measure Values'].sum()

            product_overall = \
                product[['Year', 'Measure Names', 'Measure Values']]. \
                    groupby('Year')['Measure Values'].sum()

            product_oti = product_on_time / product_overall * 100

            for value in product_oti.index.get_level_values(0):
                if int(value) != current_year:
                    score_card.loc[index, str(value) + ' Total'] = product_oti[
                        value]
                else:
                    score_card.loc[index, str(value) + ' YTD'] = product_oti[
                        value]

            product_on_time_ytd = \
                product[
                    ['Year', 'Month', 'Measure Names', 'Measure Values']].loc[
                    (product['Year'] == current_year - 1) & (
                            product['Month'] >= 10)
                    & (
                            product[
                                'Measure Names'] == '# of On Time Install')].groupby(
                    'Year')['Measure Values'].sum()

            product_overall_ytd = \
                product[
                    ['Year', 'Month', 'Measure Names', 'Measure Values']].loc[
                    (product['Year'] == current_year - 1) & (
                            product['Month'] >= 10)] \
                    .groupby('Year')['Measure Values'].sum()

            product_oti_ytd = product_on_time_ytd / product_overall_ytd * 100

            try:
                score_card.loc[index, "Q4 " + str(
                    product_oti_ytd.index.get_level_values(0)[0])[-2:]] = \
                    product_oti_ytd[
                        product_oti_ytd.index.get_level_values(0)[0]]
            except IndexError:
                score_card.loc[index, "Q4 " + str(current_year - 1)[-2:]] = ''

    return score_card


def calculate_foi(score_card, data):
    """
    Calculate FOI for scorecard
    :param score_card: scorecard structure for the metric (data frame)
    :param data: source data after hierarchy look up (data frame)
    :return: scorecard data
    """
    for index, row in score_card.iterrows():
        if row['Product Quality Scorecard'] == 'GEHC':
            gehc_parts = data.groupby('Year of Process Date')['SUM PART QTY'].sum()
            gehc_system = data.groupby('Year of Process Date')['SYSTEM ID DISTINCT CT'].sum()
            gehc_foi = gehc_parts / gehc_system
            for value in gehc_foi.index.get_level_values(0):
                if int(value) != current_year:
                    score_card.loc[index, str(value) + ' Total'] = gehc_foi[
                        value]
                else:
                    score_card.loc[index, str(value) + ' YTD'] = gehc_foi[
                        value]

            gehc_parts_ytd = \
                data.loc[
                    (data['Year of Process Date'] == current_year - 1) & (
                            data['Month'] <= current_month)].groupby('Year of Process Date')[
                    'SUM PART QTY'].sum()
            gehc_system_ytd = \
                data.loc[
                    (data['Year of Process Date'] == current_year - 1) & (
                            data['Month'] <= current_month)].groupby(
                    'Year of Process Date')[
                    'SYSTEM ID DISTINCT CT'].sum()
            gehc_foi_ytd = gehc_parts_ytd / gehc_system_ytd

            try:
                score_card.loc[index, str(
                    gehc_foi_ytd.index.get_level_values(0)[0]) + ' YTD'] = \
                    gehc_foi_ytd[gehc_foi_ytd.index.get_level_values(0)[0]]
            except IndexError:
                score_card.loc[index, str(current_year - 1) + ' YTD'] = ''
        elif row['Product Quality Scorecard'] != 'GEHC' and row[
            'Business'] == 'ALL' and row['Modality'] == 'ALL':
            business = data.loc[data['SC Business Segment'] == row[
                'Product Quality Scorecard']]
            business_parts = business.groupby('Year of Process Date')[
                'SUM PART QTY'].sum()
            business_system = business.groupby('Year of Process Date')[
                'SYSTEM ID DISTINCT CT'].sum()
            business_foi = business_parts / business_system
            for value in business_foi.index.get_level_values(0):
                if int(value) != current_year:
                    score_card.loc[index, str(value) + ' Total'] = \
                        business_foi[
                            value]
                else:
                    score_card.loc[index, str(value) + ' YTD'] = business_foi[
                        value]

            business_parts_ytd = \
                business.loc[
                    (business['Year of Process Date'] == current_year - 1) & (
                            data['Month'] <= current_month)].groupby(
                    'Year of Process Date')[
                    'SUM PART QTY'].sum()
            business_system_ytd = \
                business.loc[
                    (business['Year of Process Date'] == current_year - 1) & (
                            data['Month'] <= current_month)].groupby(
                    'Year of Process Date')[
                    'SYSTEM ID DISTINCT CT'].sum()
            business_foi_ytd = business_parts_ytd / business_system_ytd

            try:
                score_card.loc[index, str(
                    business_foi_ytd.index.get_level_values(0)[0]) + ' YTD'] = \
                    business_foi_ytd[
                        business_foi_ytd.index.get_level_values(0)[0]]
            except IndexError:
                score_card.loc[index, str(current_year - 1) + ' YTD'] = ''
        elif row['Business'] != 'ALL' and row['Modality'] == 'ALL':
            modality = data.loc[
                (data['SC Business Segment'] == row['Business']) & (
                        data['SC Modality'] == row['Product Quality Scorecard'])]
            modality_parts = modality.groupby('Year of Process Date')[
                'SUM PART QTY'].sum()
            modality_system = modality.groupby('Year of Process Date')[
                'SYSTEM ID DISTINCT CT'].sum()
            modality_foi = modality_parts / modality_system
            for value in modality_foi.index.get_level_values(0):
                if int(value) != current_year:
                    score_card.loc[index, str(value) + ' Total'] = \
                        modality_foi[
                            value]
                else:
                    score_card.loc[index, str(value) + ' YTD'] = modality_foi[
                        value]

            modality_parts_ytd = \
                modality.loc[
                    (modality['Year of Process Date'] == current_year - 1) & (
                            data['Month'] <= current_month)].groupby(
                    'Year of Process Date')[
                    'SUM PART QTY'].sum()
            modality_system_ytd = \
                modality.loc[
                    (modality['Year of Process Date'] == current_year - 1) & (
                            data['Month'] <= current_month)].groupby(
                    'Year of Process Date')[
                    'SYSTEM ID DISTINCT CT'].sum()
            modality_foi_ytd = modality_parts_ytd / modality_system_ytd

            try:
                score_card.loc[index, str(
                    modality_foi_ytd.index.get_level_values(0)[0]) + ' YTD'] = \
                    modality_foi_ytd[
                        modality_foi_ytd.index.get_level_values(0)[0]]
            except IndexError:
                score_card.loc[index, str(current_year - 1) + ' YTD'] = ''
        elif row['Business'] != 'ALL' and row['Modality'] != 'ALL' and row[
            'Product Segment'] == 'ALL':
            prod_seg = \
                data.loc[
                    (data['SC Business Segment'] == row['Business']) & (
                            data['SC Modality'] == row['Modality']) & (
                            data['SC Product Segment'] == row[
                        'Product Quality Scorecard'])]
            prod_seg_parts = prod_seg.groupby('Year of Process Date')[
                'SUM PART QTY'].sum()
            prod_seg_system = prod_seg.groupby('Year of Process Date')[
                'SYSTEM ID DISTINCT CT'].sum()
            prod_seg_foi = prod_seg_parts / prod_seg_system
            for value in prod_seg_foi.index.get_level_values(0):
                if int(value) != current_year:
                    score_card.loc[index, str(value) + ' Total'] = \
                        prod_seg_foi[
                            value]
                else:
                    score_card.loc[index, str(value) + ' YTD'] = prod_seg_foi[
                        value]

            prod_seg_parts_ytd = \
                prod_seg.loc[
                    (prod_seg['Year of Process Date'] == current_year - 1) & (
                            data['Month'] <= current_month)].groupby(
                    'Year of Process Date')[
                    'SUM PART QTY'].sum()
            prod_seg_system_ytd = \
                prod_seg.loc[
                    (prod_seg['Year of Process Date'] == current_year - 1) & (
                            data['Month'] <= current_month)].groupby(
                    'Year of Process Date')[
                    'SYSTEM ID DISTINCT CT'].sum()
            prod_seg_foi_ytd = prod_seg_parts_ytd / prod_seg_system_ytd

            try:
                score_card.loc[index, str(
                    prod_seg_foi_ytd.index.get_level_values(0)[0]) + ' YTD'] = \
                    prod_seg_foi_ytd[
                        prod_seg_foi_ytd.index.get_level_values(0)[0]]
            except IndexError:
                score_card.loc[index, str(current_year - 1) + ' YTD'] = ''
        else:
            product = \
                data.loc[
                    (data['SC Business Segment'] == row['Business']) & (
                            data['SC Modality'] ==
                            row['Modality']) & (
                            data['SC Product Segment'] == row['Product Segment']) &
                    (data['SC Product'] == row['Product Quality Scorecard'])]
            product_parts = product.groupby('Year of Process Date')[
                'SUM PART QTY'].sum()
            product_system = product.groupby('Year of Process Date')[
                'SYSTEM ID DISTINCT CT'].sum()
            product_foi = product_parts / product_system
            for value in product_foi.index.get_level_values(0):
                if int(value) != current_year:
                    score_card.loc[index, str(value) + ' Total'] = product_foi[
                        value]
                else:
                    score_card.loc[index, str(value) + ' YTD'] = product_foi[
                        value]

            product_parts_ytd = \
                product.loc[
                    (product['Year of Process Date'] == current_year - 1) & (
                            data['Month'] <= current_month)].groupby(
                    'Year of Process Date')[
                    'SUM PART QTY'].sum()
            product_system_ytd = \
                product.loc[
                    (product['Year of Process Date'] == current_year - 1) & (
                            data['Month'] <= current_month)].groupby(
                    'Year of Process Date')[
                    'SYSTEM ID DISTINCT CT'].sum()
            product_foi_ytd = product_parts_ytd / product_system_ytd

            try:
                score_card.loc[index, str(
                    product_foi_ytd.index.get_level_values(0)[0]) + ' YTD'] = \
                    product_foi_ytd[
                        product_foi_ytd.index.get_level_values(0)[0]]
            except IndexError:
                score_card.loc[index, str(current_year - 1) + ' YTD'] = ''

    return score_card


def calculate_ib_avg(score_card, data):
    current_year = datetime.datetime.now().year
    for index, row in score_card.iterrows():

        if row['Product Quality Scorecard'] == 'GEHC':
            gehc_month = data.groupby(['Year', 'Month'], as_index=False)[
                'sum'].sum()
            gehc = gehc_month.groupby('Year')['sum'].sum()

            for value in gehc.index.get_level_values(0):
                if int(value) != current_year:
                    score_card.loc[index, str(value) + ' Total'] = gehc[
                                                                       value] / \
                                                                   (
                                                                       gehc_month.loc[
                                                                           gehc_month[
                                                                               'Year'] == int(
                                                                               value)][
                                                                           'Month'].nunique())
                else:
                    score_card.loc[index, str(value) + ' YTD'] = gehc[value] / \
                                                                 (
                                                                     gehc_month.loc[
                                                                         gehc_month[
                                                                             'Year'] == int(
                                                                             value)][
                                                                         'Month'].nunique())
            gehc_ytd = data[['sum', 'Year', 'Month']].loc[
                (data['Year'] == current_year - 1) & (
                        data['Month'] == 12)]
            gehc_ytd_group = gehc_ytd.groupby('Year')['sum'].sum() / gehc_ytd[
                'Month'].nunique()
            if len(gehc_ytd_group) > 0:
                score_card.loc[index, str(
                    gehc_ytd_group.index.get_level_values(0)[0]) + ' YTD'] = \
                    gehc_ytd_group[gehc_ytd_group.index.get_level_values(0)[0]]

        elif row['Product Quality Scorecard'] != 'GEHC' and row[
            'Business'] == 'ALL' and row['Modality'] == 'ALL':
            business = data.loc[data['SC Business Segment'] == row[
                'Product Quality Scorecard']]
            business_group_month = \
                business.groupby(['Year', 'Month'], as_index=False)[
                    'sum'].sum()
            business_group = business_group_month.groupby('Year')['sum'].sum()
            for value in business_group.index.get_level_values(0):
                if int(value) != current_year:
                    score_card.loc[index, str(value) + ' Total'] = \
                        business_group[value] / \
                        (business_group_month.loc[
                             business_group_month['Year'] == int(value)][
                             'Month'].nunique())
                else:
                    score_card.loc[index, str(value) + ' YTD'] = \
                        business_group[value] / (
                            business_group_month.loc[
                                business_group_month['Year'] == int(value)][
                                'Month'].nunique())
            business_ytd = business[['sum', 'Year', 'Month']].loc[
                (business['Year'] == current_year - 1) & (
                        business['Month'] == 12)]
            business_ytd_group = business_ytd.groupby('Year')['sum'].sum() / \
                                 business_ytd['Month'].nunique()
            if len(business_ytd_group.index.get_level_values(0)) > 0:
                score_card.loc[index, str(
                    business_ytd_group.index.get_level_values(0)[
                        0]) + ' YTD'] = \
                    business_ytd_group[
                        business_ytd_group.index.get_level_values(0)[0]]
        elif row['Business'] != 'ALL' and row['Modality'] == 'ALL' and row[
            'Product Quality Scorecard'] != 'LCS':
            modality = \
                data[['SC Business Segment', 'SC Modality', 'sum', 'Year',
                      'Month']].loc[
                    (data['SC Business Segment'] == row['Business']) & (
                            data['SC Modality'] == row[
                        'Product Quality Scorecard'])]
            modality_group_month = \
                modality.groupby(['Year', 'Month'], as_index=False)[
                    'sum'].sum()
            modality_group = modality_group_month.groupby('Year')['sum'].sum()
            for value in modality_group.index.get_level_values(0):
                if int(value) != current_year:
                    score_card.loc[index, str(value) + ' Total'] = \
                        modality_group[value] / \
                        modality_group_month.loc[
                            modality_group_month['Year'] == int(value)][
                            'Month'].nunique()
                else:
                    score_card.loc[index, str(value) + ' YTD'] = \
                        modality_group[value] / \
                        modality_group_month.loc[
                            modality_group_month['Year'] == int(value)][
                            'Month'].nunique()
            modality_ytd = modality[['sum', 'Year', 'Month']].loc[
                (modality['Year'] == current_year - 1) & (
                        modality['Month'] == 12)]
            modality_ytd_group = modality_ytd.groupby('Year')['sum'].sum() / \
                                 modality_ytd['Month'].nunique()
            if len(modality_ytd_group) > 0:
                score_card.loc[index, str(
                    modality_ytd_group.index.get_level_values(0)[0]) + ' YTD'] = \
                    modality_ytd_group[
                        modality_ytd_group.index.get_level_values(0)[0]]
        elif row['Business'] != 'ALL' and row['Modality'] == 'ALL' and row[
            'Product Quality Scorecard'] == 'LCS':
            modality = \
                data[['SC Business Segment', 'SC Modality', 'sum', 'Year',
                      'Month']].loc[
                    (data['SC Business Segment'] == row['Business'])
                    & (data['SC Modality'] != 'ULTRASOUND')
                    & (data['SC Modality'] != 'OTHER')
                    & (data['SC Modality'] != 'EXCLUDED')]
            modality_group_month = \
                modality.groupby(['Year', 'Month'], as_index=False)[
                    'sum'].sum()
            modality_group = modality_group_month.groupby('Year')['sum'].sum()
            for value in modality_group.index.get_level_values(0):
                if int(value) != current_year:
                    score_card.loc[index, str(value) + ' Total'] = \
                        modality_group[value] / \
                        modality_group_month.loc[
                            modality_group_month['Year'] == int(value)][
                            'Month'].nunique()
                else:
                    score_card.loc[index, str(value) + ' YTD'] = \
                        modality_group[value] / \
                        modality_group_month.loc[
                            modality_group_month['Year'] == int(value)][
                            'Month'].nunique()
            modality_ytd = modality[['sum', 'Year', 'Month']].loc[
                (modality['Year'] == current_year - 1) & (
                        modality['Month'] == 12)]
            modality_ytd_group = modality_ytd.groupby('Year')['sum'].sum() / \
                                 modality_ytd['Month'].nunique()
            if len(modality_ytd_group) > 0:
                score_card.loc[index, str(
                    modality_ytd_group.index.get_level_values(0)[
                        0]) + ' YTD'] = \
                    modality_ytd_group[
                        modality_ytd_group.index.get_level_values(0)[0]]
        elif row['Business'] != 'ALL' and row['Modality'] != 'ALL' and row[
            'Product Segment'] == 'ALL':
            prod_seg = \
                data[['SC Business Segment', 'SC Modality',
                      'SC Product Segment', 'sum',
                      'Year', 'Month']].loc[
                    (data['SC Business Segment'] == row['Business']) & (
                            data['SC Modality'] == row['Modality']) & (
                            data['SC Product Segment'] == row[
                        'Product Quality Scorecard'])]
            prod_seg_group_month = \
                prod_seg.groupby(['Year', 'Month'], as_index=False)[
                    'sum'].sum()
            prod_seg_group = prod_seg_group_month.groupby('Year')['sum'].sum()
            for value in prod_seg_group.index.get_level_values(0):
                if int(value) != current_year:
                    score_card.loc[index, str(value) + ' Total'] = \
                        prod_seg_group[value] / \
                        prod_seg_group_month.loc[
                            prod_seg_group_month['Year'] == int(value)][
                            'Month'].nunique()
                else:
                    score_card.loc[index, str(value) + ' YTD'] = \
                        prod_seg_group[value] / \
                        prod_seg_group_month.loc[
                            prod_seg_group_month['Year'] == int(value)][
                            'Month'].nunique()
            try:
                prod_seg_ytd = prod_seg[['sum', 'Year', 'Month']].loc[
                    (prod_seg['Year'] == current_year - 1) & (
                            prod_seg['Month'] == 12)]

                prod_seg_ytd_group = prod_seg_ytd.groupby('Year')[
                                         'sum'].sum() / prod_seg_ytd[
                                         'Month'].nunique()

                score_card.loc[index, str(
                    prod_seg_ytd_group.index.get_level_values(0)[
                        0]) + ' YTD'] = \
                    prod_seg_ytd_group[
                        prod_seg_ytd_group.index.get_level_values(0)[0]]
            except KeyError:
                score_card.loc[index, str(current_year - 1) + ' YTD'] = ''
            except IndexError:
                score_card.loc[index, str(current_year - 1) + ' YTD'] = ''
        else:
            product = \
                data[['SC Business Segment', 'SC Modality',
                      'SC Product Segment', 'sum',
                      'Year', 'Month']].loc[
                    (data['SC Business Segment'] == row['Business']) & (
                            data['SC Modality'] == row['Modality']) &
                    (data['SC Product Segment'] == row['Product Segment']) &
                    (data['SC Product'] == row['Product Quality Scorecard'])]
            product_group_month = \
                product.groupby(['Year', 'Month'], as_index=False)['sum'].sum()
            product_group = product_group_month.groupby('Year')['sum'].sum()
            for value in product_group.index.get_level_values(0):
                if int(value) != current_year:
                    score_card.loc[index, str(value) + ' Total'] = \
                        product_group[value] / \
                        product_group_month.loc[
                            product_group_month['Year'] == int(value)][
                            'Month'].nunique()
                else:
                    score_card.loc[index, str(value) + ' YTD'] = product_group[
                                                                     value] / \
                                                                 product_group_month.loc[
                                                                     product_group_month[
                                                                         'Year'] == int(
                                                                         value)][
                                                                     'Month'].nunique()
            try:
                product_ytd = product[['sum', 'Year', 'Month']].loc[
                    (product['Year'] == current_year - 1) & (
                            product['Month'] == 12)]

                product_ytd_group = product_ytd.groupby('Year')['sum'].sum() / \
                                    product_ytd['Month'].nunique()

                score_card.loc[index, str(
                    product_ytd_group.index.get_level_values(0)[0]) + ' YTD'] = \
                    product_ytd_group[
                        product_ytd_group.index.get_level_values(0)[0]]
            except KeyError:
                score_card.loc[index, str(current_year - 1) + ' YTD'] = ''
            except IndexError:
                score_card.loc[index, str(current_year - 1) + ' YTD'] = ''

    return score_card


def calculate_complaint_rate(score_card, data, metric):
    for index, row in score_card.iterrows():
        if scorecard_functions.is_gehc_level(row):
            gehc_complaint = data.loc[data['Measure Names'] == '# of Complaints'].groupby('Year')[
                'Measure Values'].sum()

            gehc_ib_sum = data.loc[data['Measure Names'] == 'Average IB Over Time'].groupby(
                ['Year', 'Week Number', "Month"])['Measure Values'].sum().reset_index()
            gehc_ib = gehc_ib_sum.groupby('Year')['Measure Values'].sum()
            complaint_rate = gehc_complaint / gehc_ib
            score_card = scorecard_functions.set_complaint_rate_metric_value(index, complaint_rate,
                                                                             gehc_ib_sum,
                                                                             score_card, metric)
            gehc_ytd = scorecard_functions.get_previous_year_data(data, metric)

            gehc_complaint_ytd = \
                gehc_ytd.loc[gehc_ytd['Measure Names'] == '# of Complaints'].groupby(['Year'])[
                    'Measure Values'].sum()

            gehc_ib_ytd_sum = \
                gehc_ytd.loc[gehc_ytd['Measure Names'] == 'Average IB Over Time'].groupby(
                    ['Year', "Month"])['Measure Values'].sum().reset_index()

            gehc_ib_ytd = gehc_ib_ytd_sum.groupby('Year')['Measure Values'].sum() / (
                gehc_ib_ytd_sum['Month'].nunique())

            complaint_rate_ytd = gehc_complaint_ytd / gehc_ib_ytd

            score_card = scorecard_functions.set_metric_value_previous(complaint_rate_ytd, index,
                                                                       score_card)

        elif scorecard_functions.is_business_level(row):
            business = scorecard_functions.get_business_data(data, row, False, 0, metric)

            complaint_business = \
                business.loc[business['Measure Names'] == '# of Complaints'].groupby('Year')[
                    'Measure Values'].sum()

            ib_business_sum = \
                business.loc[business['Measure Names'] == 'Average IB Over Time'].groupby(
                    ['Year', "Month"])['Measure Values'].sum().reset_index()

            ib_business = ib_business_sum.groupby('Year')['Measure Values'].sum()

            complaint_rate_business = complaint_business / ib_business

            score_card = scorecard_functions.set_complaint_rate_metric_value(index,
                                                                             complaint_rate_business,
                                                                             ib_business_sum,
                                                                             score_card, metric)
            business_ytd = scorecard_functions.get_business_data(data, row, True, 0, metric)

            business_complaint_ytd = \
                business_ytd.loc[business_ytd['Measure Names'] == '# of Complaints'].groupby(
                    'Year')[
                    'Measure Values'].sum()

            business_ib_ytd_sum = \
                business_ytd.loc[business_ytd['Measure Names'] == 'Average IB Over Time'].groupby(
                    ['Year', "Month"])['Measure Values'].sum().reset_index()

            business_ib_ytd = business_ib_ytd_sum.groupby('Year')['Measure Values'].sum() / (
                business_ib_ytd_sum['Month'].nunique())
            business_complaint_rate_ytd = business_complaint_ytd / business_ib_ytd

            score_card = scorecard_functions.set_metric_value_previous(business_complaint_rate_ytd,
                                                                       index, score_card)

        elif scorecard_functions.is_modality_level(row=row, is_lcs=False):
            modality = scorecard_functions.get_modality_data(data=data, row=row, is_lcs=False,
                                                             is_previous_year=False,
                                                             number_of_weeks=0, metric=metric)

            complaint_modality = \
                modality.loc[modality['Measure Names'] == '# of Complaints'].groupby('Year')[
                    'Measure Values'].sum()

            ib_modality_sum = \
                modality.loc[modality['Measure Names'] == 'Average IB Over Time'].groupby(
                    ['Year', "Month"])['Measure Values'].sum().reset_index()
            ib_modality = ib_modality_sum.groupby('Year')['Measure Values'].sum()

            complaint_rate_modality = complaint_modality / ib_modality

            score_card = scorecard_functions.set_complaint_rate_metric_value(index,
                                                                             complaint_rate_modality,
                                                                             ib_modality_sum,
                                                                             score_card, metric)
            modality_previous_ytd = scorecard_functions.get_modality_data(data=data, row=row,
                                                                          is_lcs=False,
                                                                          is_previous_year=True,
                                                                          number_of_weeks=0,
                                                                          metric=metric)

            modality_complaint_ytd = modality_previous_ytd[
                modality_previous_ytd['Measure Names'] == '# of Complaints'].groupby('Year')[
                'Measure Values'].sum()

            modality_ib_ytd_sum = \
                modality_previous_ytd.loc[
                    modality_previous_ytd['Measure Names'] == 'Average IB Over Time'].groupby(
                    ['Year', "Month"])['Measure Values'].sum().reset_index()

            modality_ib_ytd = modality_ib_ytd_sum.groupby('Year')['Measure Values'].sum() / (
                modality_ib_ytd_sum['Month'].nunique())

            modality_complaint_rate_ytd = modality_complaint_ytd / modality_ib_ytd
            score_card = scorecard_functions.set_metric_value_previous(modality_complaint_rate_ytd,
                                                                       index, score_card)

        elif scorecard_functions.is_modality_level(row=row, is_lcs=True):

            modality = scorecard_functions.get_modality_data(data=data, row=row, is_lcs=True,
                                                             is_previous_year=False,
                                                             number_of_weeks=0, metric=metric)

            complaint_modality = \
                modality.loc[modality['Measure Names'] == '# of Complaints'].groupby('Year')[
                    'Measure Values'].sum()

            ib_modality_sum = \
                modality.loc[modality['Measure Names'] == 'Average IB Over Time'].groupby(
                    ['Year', "Month"])['Measure Values'].sum().reset_index()
            ib_modality = ib_modality_sum.groupby('Year')['Measure Values'].sum()

            complaint_rate_modality = complaint_modality / ib_modality

            score_card = scorecard_functions.set_complaint_rate_metric_value(index,
                                                                             complaint_rate_modality,
                                                                             ib_modality_sum,
                                                                             score_card, metric)
            modality_previous_ytd = scorecard_functions.get_modality_data(data=data, row=row,
                                                                          is_lcs=True,
                                                                          is_previous_year=True,
                                                                          number_of_weeks=0,
                                                                          metric=metric)

            modality_complaint_ytd = modality_previous_ytd.loc[
                modality_previous_ytd['Measure Names'] == '# of Complaints'].groupby('Year')[
                'Measure Values'].sum()

            modality_ib_ytd_sum = modality_previous_ytd.loc[
                modality_previous_ytd['Measure Names'] == 'Average IB Over Time'].groupby(
                ['Year', 'Month'])['Measure Values'].sum().reset_index()

            modality_ib_ytd = modality_ib_ytd_sum.groupby('Year')['Measure Values'].sum() / (
                modality_ib_ytd_sum['Month'].nunique())

            modality_complaint_rate_ytd = modality_complaint_ytd / modality_ib_ytd

            score_card = scorecard_functions.set_metric_value_previous(modality_complaint_rate_ytd,
                                                                       index, score_card)

        elif scorecard_functions.is_product_segment_level(row):
            prod_seg = scorecard_functions.get_product_segment_data(data, row, False, 0, metric)

            complaint_prod_seg = \
                prod_seg.loc[prod_seg['Measure Names'] == '# of Complaints'].groupby('Year')[
                    'Measure Values'].sum()

            ib_prod_seg_sum = \
                prod_seg.loc[prod_seg['Measure Names'] == 'Average IB Over Time'].groupby(
                    ['Year', "Month"])['Measure Values'].sum().reset_index()
            ib_prod_seg = ib_prod_seg_sum.groupby('Year')['Measure Values'].sum()

            complaint_rate_prod_seg = complaint_prod_seg / ib_prod_seg

            score_card = scorecard_functions.set_complaint_rate_metric_value(index,
                                                                             complaint_rate_prod_seg,
                                                                             ib_prod_seg_sum,
                                                                             score_card, metric)
            prod_seg_ytd_previous = scorecard_functions.get_product_segment_data(data, row, True, 0,
                                                                                 metric)

            prod_seg_complaint_ytd = prod_seg_ytd_previous.loc[
                prod_seg_ytd_previous['Measure Names'] == '# of Complaints'].groupby('Year')[
                'Measure Values'].sum()

            prod_seg_ib_ytd_sum = prod_seg_ytd_previous.loc[
                prod_seg_ytd_previous['Measure Names'] == 'Average IB Over Time'].groupby(
                ['Year', 'Month'])['Measure Values'].sum().reset_index()

            prod_seg_ib_ytd = prod_seg_ib_ytd_sum.groupby('Year')['Measure Values'].sum() / (
                prod_seg_ib_ytd_sum['Month'].nunique())
            prod_seg_complaint_rate_ytd = prod_seg_complaint_ytd / prod_seg_ib_ytd
            score_card = scorecard_functions.set_metric_value_previous(prod_seg_complaint_rate_ytd,
                                                                       index, score_card)
        else:
            product = scorecard_functions.get_product_data(data, row, False, 0, metric)

            complaint_product = \
                product.loc[product['Measure Names'] == '# of Complaints'].groupby('Year')[
                    'Measure Values'].sum()

            ib_product_sum = \
                product.loc[product['Measure Names'] == 'Average IB Over Time'].groupby(
                    ['Year', "Month"])['Measure Values'].sum().reset_index()
            ib_product = ib_product_sum.groupby('Year')['Measure Values'].sum()

            complaint_rate_product = complaint_product / ib_product

            score_card = scorecard_functions.set_complaint_rate_metric_value(index,
                                                                             complaint_rate_product,
                                                                             ib_product_sum,
                                                                             score_card, metric)

            product_previous_ytd = scorecard_functions.get_product_data(data, row, True, 0, metric)

            product_complaint_ytd = product_previous_ytd.loc[
                product_previous_ytd['Measure Names'] == '# of Complaints'].groupby('Year')[
                'Measure Values'].sum()

            product_ib_ytd_sum = product_previous_ytd.loc[
                product_previous_ytd['Measure Names'] == 'Average IB Over Time'].groupby(
                ['Year', "Month"])['Measure Values'].sum().reset_index()

            product_ib_ytd = product_ib_ytd_sum.groupby('Year')['Measure Values'].sum() / (
                product_ib_ytd_sum['Month'].nunique())
            product_complaint_rate_ytd = product_complaint_ytd / product_ib_ytd
            score_card = scorecard_functions.set_metric_value_previous(product_complaint_rate_ytd,
                                                                       index, score_card)

    return score_card


def calculate_complaint_rate_chu(score_card, data, metric):
    for index, row in score_card.iterrows():
        if scorecard_functions.is_gehc_level(row):
            gehc_complaint = data.loc[data['Measure Names'] == '# of Complaints'].groupby('Year')[
                'Measure Values'].sum()
            gehc_ib_sum = \
                data.loc[data['Measure Names'] == 'Average IB Over Time'].groupby(
                    ['Year', 'Quarter'])[
                    'Measure Values'].sum().reset_index()
            gehc_ib = gehc_ib_sum.groupby('Year')['Measure Values'].sum()
            complaint_rate = gehc_complaint / gehc_ib
            score_card = scorecard_functions.set_complaint_rate_metric_value(index, complaint_rate,
                                                                             gehc_ib_sum,
                                                                             score_card, metric)
            gehc_ytd = scorecard_functions.get_previous_year_data(data, metric)
            gehc_complaint_ytd = \
                gehc_ytd.loc[gehc_ytd['Measure Names'] == '# of Complaints'].groupby(['Year'])[
                    'Measure Values'].sum()

            gehc_ib_ytd_sum = \
                gehc_ytd.loc[gehc_ytd['Measure Names'] == 'Average IB Over Time'].groupby(
                    ['Year', "Quarter"])['Measure Values'].sum().reset_index()

            gehc_ib_ytd = gehc_ib_ytd_sum.groupby('Year')['Measure Values'].sum() / (
                gehc_ib_ytd_sum['Quarter'].nunique())

            complaint_rate_ytd = gehc_complaint_ytd / gehc_ib_ytd
            score_card = scorecard_functions.set_metric_value_previous(complaint_rate_ytd, index,
                                                                       score_card)

        elif scorecard_functions.is_business_level(row):
            business = scorecard_functions.get_business_data(data, row, False, 0, metric)

            complaint_business = \
                business.loc[business['Measure Names'] == '# of Complaints'].groupby('Year')[
                    'Measure Values'].sum()

            ib_business_sum = \
                business.loc[business['Measure Names'] == 'Average IB Over Time'].groupby(
                    ['Year', "Quarter"])['Measure Values'].sum().reset_index()

            ib_business = ib_business_sum.groupby('Year')[
                'Measure Values'].sum()

            complaint_rate_business = complaint_business / ib_business

            score_card = scorecard_functions \
                .set_complaint_rate_metric_value(index,
                                                 complaint_rate_business, ib_business_sum,
                                                 score_card, metric)
            business_ytd = scorecard_functions.get_business_data(data, row, True, 0, metric)
            business_complaint_ytd = \
                business_ytd.loc[business_ytd['Measure Names'] == '# of Complaints'].groupby(
                    'Year')[
                    'Measure Values'].sum()

            business_ib_ytd_sum = \
                business_ytd.loc[business_ytd['Measure Names'] == 'Average IB Over Time'].groupby(
                    ['Year', "Quarter"])['Measure Values'].sum().reset_index()

            business_ib_ytd = business_ib_ytd_sum.groupby('Year')['Measure Values'].sum() / (
                business_ib_ytd_sum['Quarter'].nunique())
            business_complaint_rate_ytd = business_complaint_ytd / business_ib_ytd

            score_card = scorecard_functions.set_metric_value_previous(business_complaint_rate_ytd,
                                                                       index, score_card)

        elif scorecard_functions.is_modality_level(row=row, is_lcs=False):
            modality = scorecard_functions.get_modality_data(data=data, row=row, is_lcs=False,
                                                             is_previous_year=False,
                                                             number_of_weeks=0, metric=metric)
            complaint_modality = \
                modality.loc[modality['Measure Names'] == '# of Complaints'].groupby('Year')[
                    'Measure Values'].sum()

            ib_modality_sum = modality.loc[modality[
                                               'Measure Names'] == 'Average IB Over Time'].groupby(
                ['Year', "Quarter"])['Measure Values'].sum().reset_index()
            ib_modality = ib_modality_sum.groupby('Year')['Measure Values'].sum()

            complaint_rate_modality = complaint_modality / ib_modality

            score_card = scorecard_functions.set_complaint_rate_metric_value(index,
                                                                             complaint_rate_modality,
                                                                             ib_modality_sum,
                                                                             score_card, metric)
            modality_previous_ytd = scorecard_functions.get_modality_data(data=data, row=row,
                                                                          is_lcs=False,
                                                                          is_previous_year=True,
                                                                          number_of_weeks=0,
                                                                          metric=metric)

            modality_complaint_ytd = modality_previous_ytd.loc[
                modality_previous_ytd['Measure Names'] == '# of Complaints'].groupby('Year')[
                'Measure Values'].sum()

            modality_ib_ytd_sum = \
                modality_previous_ytd.loc[
                    modality_previous_ytd['Measure Names'] == 'Average IB Over Time'].groupby(
                    ['Year', "Quarter"])['Measure Values'].sum().reset_index()

            modality_ib_ytd = modality_ib_ytd_sum.groupby('Year')[
                                  'Measure Values'].sum() / \
                              (modality_ib_ytd_sum['Quarter'] \
                               .nunique())

            modality_complaint_rate_ytd = modality_complaint_ytd / modality_ib_ytd

            score_card = scorecard_functions.set_metric_value_previous(modality_complaint_rate_ytd,
                                                                       index, score_card)

        else:
            product = scorecard_functions.get_product_data(data, row, False, 0, metric)

            complaint_product = \
                product.loc[product['Measure Names'] == '# of Complaints'].groupby('Year')[
                    'Measure Values'].sum()

            ib_product_sum = product.loc[
                product['Measure Names'] == 'Average IB Over Time'].groupby(['Year', "Quarter"])[
                'Measure Values'].sum().reset_index()
            ib_product = ib_product_sum.groupby('Year')['Measure Values'].sum()

            complaint_rate_product = complaint_product / ib_product

            score_card = scorecard_functions.set_complaint_rate_metric_value(index,
                                                                             complaint_rate_product,
                                                                             ib_product_sum,
                                                                             score_card, metric)

            product_previous_ytd = scorecard_functions.get_product_data(data, row, True, 0, metric)

            product_complaint_ytd = product_previous_ytd.loc[
                product_previous_ytd['Measure Names'] == '# of Complaints'].groupby('Year')[
                'Measure Values'].sum()

            product_ib_ytd_sum = product_previous_ytd.loc[
                product_previous_ytd['Measure Names'] == 'Average IB Over Time'].groupby(
                ['Year', "Quarter"])['Measure Values'].sum().reset_index()

            product_ib_ytd = product_ib_ytd_sum.groupby('Year')['Measure Values'].sum() / (
                product_ib_ytd_sum['Quarter'].nunique())
            product_complaint_rate_ytd = product_complaint_ytd / product_ib_ytd

            score_card = scorecard_functions.set_metric_value_previous(product_complaint_rate_ytd,
                                                                       index, score_card)

    return score_card


def setup_mr_route_groups(data, route_data, prod_route):
    data['Measure Values'].fillna(0, inplace=True)
    time_periods = data['Period'].unique()
    for time_period in time_periods:
        data.loc[data['Period'] == time_period, "Year"] = \
            time_period.split('M')[0]
        data.loc[data['Period'] == time_period, "Month"] = \
            time_period.split('M')[1]
    data['Year'] = data['Year'].astype(int)
    data['Month'] = data['Month'].astype(int)

    route_parts = data['Route & Part Combo'].unique()
    for route_part in route_parts:
        mapping_data = route_data.loc[
            route_data['Route & Part Number'] == route_part]
        if len(mapping_data) > 0:
            if len(mapping_data) == 1:
                data.loc[
                    data['Route & Part Combo'] == route_part, "Grouping"] = \
                    mapping_data['Grouping Name'].values[0]
                data.loc[data['Route & Part Combo'] == route_part, "Product"] = \
                    mapping_data['Product'].values[0]
            else:
                part_data = pd.DataFrame(
                    data.loc[data['Route & Part Combo'] == route_part])
                product_names = mapping_data['Product'].unique()
                for product_name in product_names:
                    part_data['Product'] = product_name
                    part_data['Grouping'] = \
                        mapping_data.loc[
                            mapping_data['Product'] == product_name][
                            'Grouping Name'].values[0]
                    data = data.append(part_data)
    data.fillna('drop', inplace=True)
    data = data.loc[(data['Grouping'] != 'drop') & (data['Product'] != 'drop')]
    data.loc[data['Product'] == 'MRPET', "Product"] = 'PETMR'
    groups = data['Grouping'].unique()
    data = data.groupby(
        ['Measure Names', 'Period', 'Grouping', 'Product', 'Month', 'Year']) \
        ['Measure Values'].sum().reset_index()
    for group in groups:
        group_data = prod_route.loc[prod_route['Grouping Name'] == group]
        if len(group_data) > 0:
            products = group_data['Product'].unique()
            for product in products:
                data.loc[((data['Grouping'] == group) & (
                        data['Product'] == product)), "HLA"] = group_data['HLA'] \
                    .values[0]
                data.loc[((data['Grouping'] == group) & (
                        data['Product'] == product)),
                         "Use to calculate # units shipped?"] = \
                    group_data['Use to calculate # units shipped?'].values[
                        0]
                data.loc[((data['Grouping'] == group) & (
                        data['Product'] == product)), "Series or Parallel"] = \
                    group_data['Series or Parallel'].values[0]
                data.loc[((data['Grouping'] == group) & (
                        data['Product'] == product)), "Process"] = \
                    group_data['Process'].values[0]
                data.loc[((data['Grouping'] == group) & (
                        data['Product'] == product)), "# Per System"] = \
                    group_data['# Per System'].values[0]
    months = data['Month'].unique()
    for month in months:
        if month % 3 > 0:
            data.loc[data['Month'] == month, "Quarter"] = 'Q' + str(month // 3 + 1)
        else:
            data.loc[data['Month'] == month, "Quarter"] = 'Q' + str(month // 3)
    data['Duration'] = data['Year'].astype(str) + data['Quarter']
    return data


def calculate_group_aggregates(data):
    group_names = data['Grouping'].unique()
    for group_name in group_names:
        products = data.loc[(data['Grouping'] == group_name)][
            'Product'].unique()
        for product in products:
            time_periods = data.loc[(data['Grouping'] == group_name) & (
                    data['Product'] == product)]['Period'].unique()
            for time_period in time_periods:
                group_data = data.loc[(data['Grouping'] == group_name) & (
                        data['Product'] == product) &
                                      (data['Period'] == time_period)]
                measure_data = pd.DataFrame(columns=group_data.columns)
                measure_data['Measure Names'] = pd.Series('FPY')
                measure_data['Period'] = time_period
                measure_data['Year'] = group_data['Year'].values[0]
                measure_data['Month'] = group_data['Month'].values[0]
                measure_data['Grouping'] = group_data['Grouping'].values[0]
                measure_data['HLA'] = group_data['HLA'].values[0]
                measure_data['Quarter'] = group_data['Quarter'].values[0]
                measure_data['Duration'] = group_data['Duration'].values[0]
                measure_data['Series or Parallel'] = \
                    group_data['Series or Parallel'].values[0]
                measure_data['Process'] = group_data['Process'].values[0]
                measure_data['# Per System'] = \
                    group_data['# Per System'].values[0]
                measure_data['Use to calculate # units shipped?'] = \
                    group_data['Use to calculate # units shipped?'].values[0]
                measure_data['Product'] = product
                if int(group_data.loc[group_data[
                                          'Measure Names'] == 'Route_Total_Size'][
                           'Measure Values'].values[
                           0]) > 0:
                    measure_data['Measure Values'] = int(
                        group_data.loc[group_data['Measure Names'] ==
                                       'Route First Pass Size'][
                            'Measure Values'].values[0]) / \
                                                     int(group_data.loc[
                                                             group_data[
                                                                 'Measure Names'] == 'Route_Total_Size'] \
                                                             [
                                                             'Measure Values'].values[
                                                             0]) ** \
                                                     (int(group_data[
                                                              '# Per System'].values[
                                                              0]))
                else:
                    measure_data['Measure Values'] = 0
                data = data.append(measure_data)
                measure_data = pd.DataFrame(columns=group_data.columns)
                measure_data['Measure Names'] = pd.Series('DPU')
                measure_data['Period'] = time_period
                measure_data['Year'] = group_data['Year'].values[0]
                measure_data['Month'] = group_data['Month'].values[0]
                measure_data['Grouping'] = group_data['Grouping'].values[0]
                measure_data['HLA'] = group_data['HLA'].values[0]
                measure_data['Quarter'] = group_data['Quarter'].values[0]
                measure_data['Duration'] = group_data['Duration'].values[0]
                measure_data['Series or Parallel'] = \
                    group_data['Series or Parallel'].values[0]
                measure_data['Process'] = group_data['Process'].values[0]
                measure_data['# Per System'] = \
                    group_data['# Per System'].values[0]
                measure_data['Use to calculate # units shipped?'] = \
                    group_data['Use to calculate # units shipped?'].values[0]
                measure_data['Product'] = product
                if int(group_data.loc[group_data[
                                          'Measure Names'] == 'Route_Total_Size'][
                           'Measure Values'].values[
                           0]) > 0:
                    measure_data['Measure Values'] = int(
                        group_data['# Per System'].values[0]) * int(
                        group_data.loc[group_data['Measure Names'] ==
                                       'Count of Defects'][
                            'Measure Values'].values[0]) / \
                                                     int(group_data.loc[
                                                             group_data[
                                                                 'Measure Names'] ==
                                                             'Route_Total_Size'] \
                                                             [
                                                             'Measure Values'].values[
                                                             0])
                else:
                    measure_data['Measure Values'] = 0
                data = data.append(measure_data)
    return data


def aggregate_parallel_groups(data, parallel_steps):
    parallel_data = data.loc[data['Series or Parallel'] == 'Parallel']
    if len(parallel_data) > 0:
        products = parallel_data['Product'].unique()
        for product in products:
            time_periods = parallel_data.loc[
                (parallel_data['Series or Parallel'] == 'Parallel')
                & (parallel_data['Product'] == product)]['Period'].unique()
            for time_period in time_periods:
                processes = parallel_data.loc[
                    (parallel_data['Series or Parallel'] == 'Parallel')
                    & (parallel_data['Product'] == product)
                    & (parallel_data['Period'] == time_period)][
                    'Process'].unique()
                group_aggregation = 0
                summation = 0
                process_calculation = 0
                prod = 1
                sum = 0
                average = 0
                no_of_steps = 0
                for process in processes:
                    if process is not None and process != '':

                        process_data = parallel_data.loc[
                            (parallel_data['Series or Parallel'] == 'Parallel')
                            & (parallel_data['Product'] == product)
                            & (parallel_data['Period'] == time_period)
                            & (parallel_data['Process'] == process)]
                        groups = process_data['Grouping'].unique()
                        for group in groups:
                            group_data = parallel_data.loc[
                                (parallel_data['Product'] == product)
                                & (parallel_data['Period'] == time_period)
                                & (parallel_data['Process'] == process)
                                & (parallel_data['Grouping'] == group)]
                            no_of_steps = int(parallel_steps.loc[(
                                                                         parallel_steps[
                                                                             'Process'] == process)
                                                                 & (
                                                                         parallel_steps[
                                                                             'Product'] == product)] \
                                                  ['Number of Steps'].values[
                                                  0])
                            for index, row in group_data.iterrows():
                                if row['Measure Names'] == 'FPY':
                                    prod = prod * float(row['Measure Values'])
                                if row['Measure Names'] == 'Route_Total_Size':
                                    sum = int(row['Measure Values'])
                            average = sum / no_of_steps
                            group_aggregation = group_aggregation + (
                                    prod * average)
                            summation = summation + average

                if summation > 0:

                    process_calculation = group_aggregation / summation

                else:
                    process_calculation = 0
                measure_data = pd.DataFrame(columns=process_data.columns)
                if len(process_data) > 0:
                    measure_data['Measure Names'] = pd.Series('FPY')
                    measure_data['Measure Values'] = process_calculation
                    measure_data['Period'] = time_period
                    measure_data['Year'] = process_data['Year'].values[0]
                    measure_data['Month'] = process_data['Month'].values[0]
                    measure_data['Quarter'] = process_data['Quarter'].values[0]
                    measure_data['Duration'] = process_data['Duration'].values[0]
                    measure_data['Grouping'] = pd.Series(
                        product + process_data['HLA'].values[0] + ' Total FPY')
                    measure_data['HLA'] = process_data['HLA'].values[0]
                    measure_data['Series or Parallel'] = pd.Series(
                        'Aggregation of Parallel Process')
                    measure_data['Process'] = pd.Series(
                        'Aggregation of Parallel Process')
                    measure_data['# Per System'] = \
                        process_data['# Per System'].values[0]
                    measure_data[
                        'Use to calculate # units shipped?'] = pd.Series('No')
                    measure_data['Product'] = product
                    data = data.append(measure_data)
    return data


def label_child(row, level):
    """
    Determines if a given row has a child element or not
    :param row: scorecard row
    :return: Yes - if child is present, No - if no child is present
    """
    has_child = "No"
    if (level == "product" and (row['Business'] == "ALL" or row['Modality'] == "ALL" or row[
        'Product Segment'] == "ALL")) or (level == "modality" and row['Business'] == "ALL") or (
            level == "chu-product" and (row["Business"] == "ALL" or row["Modality"] == "ALL")):
        has_child = 'Yes'
    return has_child


def calculate_yield(data, scorecard):
    for index, row in scorecard.iterrows():
        if row['Modality'] != "ALL" and row['Business'] != 'ALL' and row[
            'Product Segment'] != 'ALL':
            product = \
                data.loc[
                    (data['SC Business Segment'] == row['Business']) & (
                            data['SC Modality'] ==
                            row['Modality']) & (
                            data['SC Product Segment'] == row['Product Segment']) &
                    (data['SC Product'] == row['Product Quality Scorecard'])]

            # calculate yield for 2018
            product_route_first_size_18 = \
                product.loc[(product['Year'] == 2018) & (
                        product['Measure Names'] == 'Route First Pass Size')].groupby(
                    'Quarter')[
                    'Measure Values'].sum()

            product_route_total_size_18 = \
                product.loc[(product['Year'] == 2018) & (
                        product['Measure Names'] == 'Route_Total_Qty')].groupby(
                    'Quarter')[
                    'Measure Values'].sum()

            product_yield_18 = (
                    product_route_first_size_18 / product_route_total_size_18).reset_index()
            product_yield_18.fillna('inf', inplace=True)
            if len(product_yield_18.loc[product_yield_18['Quarter'] == 'Q3'][
                       'Measure Values'].values) > 0:
                if product_yield_18.loc[product_yield_18['Quarter'] == 'Q3'][
                    'Measure Values'].values[0] != '' and \
                        product_yield_18.loc[product_yield_18['Quarter'] == 'Q3'][
                            'Measure Values'].values[0] != 'inf':
                    scorecard.loc[index, 'Yield Q3 2018'] = round(
                        product_yield_18.loc[product_yield_18['Quarter'] == 'Q3'][
                            'Measure Values'].values[0] * 100, 2)
                else:
                    scorecard.loc[index, 'Yield Q3 2018'] = \
                        product_yield_18.loc[product_yield_18['Quarter'] == 'Q3'][
                            'Measure Values'].values[0]
            if len(product_yield_18.loc[product_yield_18['Quarter'] == 'Q4'][
                       'Measure Values'].values) > 0:
                if product_yield_18.loc[product_yield_18['Quarter'] == 'Q4'][
                    'Measure Values'].values[0] != '' and \
                        product_yield_18.loc[product_yield_18['Quarter'] == 'Q4'][
                            'Measure Values'].values[0] != 'inf':
                    scorecard.loc[index, 'Yield Q4 2018'] = round(
                        product_yield_18.loc[product_yield_18['Quarter'] == 'Q4'][
                            'Measure Values'].values[0] * 100, 2)
                else:
                    scorecard.loc[index, 'Yield Q4 2018'] = \
                        product_yield_18.loc[product_yield_18['Quarter'] == 'Q4'][
                            'Measure Values'].values[0]

            # calculate yield for 2019
            product_route_first_size_19 = \
                product.loc[(product['Year'] == 2019) & (
                        product['Measure Names'] == 'Route First Pass Size')].groupby(
                    'Year')[
                    'Measure Values'].sum()
            product_route_total_size_19 = \
                product.loc[(product['Year'] == 2019) & (
                        product['Measure Names'] == 'Route_Total_Qty')].groupby(
                    'Year')[
                    'Measure Values'].sum()
            product_yield_19 = (
                    product_route_first_size_19 / product_route_total_size_19).reset_index()
            product_yield_19.fillna('inf', inplace=True)
            if len(product_yield_19['Measure Values'].values) > 0:
                if product_yield_19['Measure Values'].values[0] != '' and \
                        product_yield_19['Measure Values'].values[
                            0] != 'inf':
                    scorecard.loc[index, 'Yield 2019 YTD'] = round(
                        product_yield_19['Measure Values'].values[0] * 100,
                        2)
                else:
                    scorecard.loc[index, 'Yield 2019 YTD'] = \
                        product_yield_19['Measure Values'].values[0]
        elif row['Modality'] != "ALL" and row['Business'] != 'ALL' and row[
            'Product Segment'] == 'ALL':
            prod_seg = \
                data.loc[
                    (data['SC Business Segment'] == row['Business']) & (
                            data['SC Modality'] ==
                            row['Modality']) &
                    (data['SC Product Segment'] == row['Product Quality Scorecard'])]

            # calculate yield for 2018
            prod_seg_route_first_size_18 = \
                prod_seg.loc[
                    (prod_seg['Year'] == 2018) & (
                            prod_seg['Measure Names'] == 'Route First Pass Size')].groupby(
                    'Quarter')[
                    'Measure Values'].sum()

            prod_seg_route_total_size_18 = \
                prod_seg.loc[(prod_seg['Year'] == 2018) & (
                        prod_seg['Measure Names'] == 'Route_Total_Qty')].groupby(
                    'Quarter')[
                    'Measure Values'].sum()

            prod_seg_yield_18 = (
                    prod_seg_route_first_size_18 / prod_seg_route_total_size_18).reset_index()
            prod_seg_yield_18.fillna('inf', inplace=True)
            if len(prod_seg_yield_18.loc[prod_seg_yield_18['Quarter'] == 'Q3'][
                       'Measure Values'].values) > 0:
                if prod_seg_yield_18.loc[prod_seg_yield_18['Quarter'] == 'Q3'][
                    'Measure Values'].values[0] != '' and \
                        prod_seg_yield_18.loc[prod_seg_yield_18['Quarter'] == 'Q3'][
                            'Measure Values'].values[0] != 'inf':
                    scorecard.loc[index, 'Yield Q3 2018'] = round(
                        prod_seg_yield_18.loc[prod_seg_yield_18['Quarter'] == 'Q3'][
                            'Measure Values'].values[0] * 100, 2)
                else:
                    scorecard.loc[index, 'Yield Q3 2018'] = \
                        prod_seg_yield_18.loc[prod_seg_yield_18['Quarter'] == 'Q3'][
                            'Measure Values'].values[0]

            if len(prod_seg_yield_18.loc[prod_seg_yield_18['Quarter'] == 'Q4'][
                       'Measure Values'].values) > 0:
                if prod_seg_yield_18.loc[prod_seg_yield_18['Quarter'] == 'Q4'][
                    'Measure Values'].values[0] != '' and \
                        prod_seg_yield_18.loc[prod_seg_yield_18['Quarter'] == 'Q4'][
                            'Measure Values'].values[0] != 'inf':
                    scorecard.loc[index, 'Yield Q4 2018'] = round(
                        prod_seg_yield_18.loc[prod_seg_yield_18['Quarter'] == 'Q4'][
                            'Measure Values'].values[0] * 100, 2)
                else:
                    scorecard.loc[index, 'Yield Q4 2018'] = \
                        prod_seg_yield_18.loc[prod_seg_yield_18['Quarter'] == 'Q4'][
                            'Measure Values'].values[0]

            # calculate yield for 2019
            prod_seg_route_first_size_19 = \
                prod_seg.loc[
                    (prod_seg['Year'] == 2019) & (
                            prod_seg['Measure Names'] == 'Route First Pass Size')].groupby(
                    'Year')[
                    'Measure Values'].sum()
            prod_seg_route_total_size_19 = \
                prod_seg.loc[(prod_seg['Year'] == 2019) & (
                        prod_seg['Measure Names'] == 'Route_Total_Qty')].groupby(
                    'Year')[
                    'Measure Values'].sum()
            prod_seg_yield_19 = (
                    prod_seg_route_first_size_19 / prod_seg_route_total_size_19).reset_index()
            prod_seg_yield_19.fillna('inf', inplace=True)
            if len(prod_seg_yield_19['Measure Values'].values) > 0:
                if prod_seg_yield_19['Measure Values'].values[0] != '' and \
                        prod_seg_yield_19['Measure Values'].values[
                            0] != 'inf':
                    scorecard.loc[index, 'Yield 2019 YTD'] = round(
                        prod_seg_yield_19['Measure Values'].values[0] * 100,
                        2)
                else:
                    scorecard.loc[index, 'Yield 2019 YTD'] = \
                        prod_seg_yield_19['Measure Values'].values[
                            0]
            else:
                scorecard.loc[index, 'Yield 2019 YTD'] = " "

    return scorecard


def calculate_dpu(data, scorecard):
    for index, row in scorecard.iterrows():
        if row['Modality'] != "ALL" and row['Business'] != 'ALL' and row[
            'Product Segment'] != 'ALL':
            product = \
                data.loc[
                    (data['SC Business Segment'] == row['Business']) & (
                            data['SC Modality'] ==
                            row['Modality']) & (
                            data['SC Product Segment'] == row['Product Segment']) &
                    (data['SC Product'] == row['Product Quality Scorecard'])]

            # calculate dpu for 2018
            product_defect_18 = \
                product.loc[(product['Year'] == 2018) & (
                        product['Measure Names'] == 'Count of Defects')].groupby(
                    'Quarter')[
                    'Measure Values'].sum()

            product_route_total_size_18 = \
                product.loc[(product['Year'] == 2018) & (
                        product['Measure Names'] == 'Route_Total_Qty')].groupby(
                    'Quarter')[
                    'Measure Values'].sum()

            product_dpu_18 = (product_defect_18 / product_route_total_size_18).reset_index()
            product_dpu_18.fillna('inf', inplace=True)
            if len(product_dpu_18.loc[product_dpu_18['Quarter'] == 'Q3'][
                       'Measure Values'].values) > 0:
                if product_dpu_18.loc[product_dpu_18['Quarter'] == 'Q3'][
                    'Measure Values'].values[0] != '' and \
                        product_dpu_18.loc[product_dpu_18['Quarter'] == 'Q3'][
                            'Measure Values'].values[0] != 'inf':
                    scorecard.loc[index, 'DPU Q3 2018'] = round(
                        product_dpu_18.loc[product_dpu_18['Quarter'] == 'Q3'][
                            'Measure Values'].values[0], 2)
                else:
                    scorecard.loc[index, 'DPU Q3 2018'] = \
                        product_dpu_18.loc[product_dpu_18['Quarter'] == 'Q3'][
                            'Measure Values'].values[0]
            if len(product_dpu_18.loc[product_dpu_18['Quarter'] == 'Q4'][
                       'Measure Values'].values) > 0:
                if product_dpu_18.loc[product_dpu_18['Quarter'] == 'Q4'][
                    'Measure Values'].values[0] != '' and \
                        product_dpu_18.loc[product_dpu_18['Quarter'] == 'Q4'][
                            'Measure Values'].values[0] != 'inf':
                    scorecard.loc[index, 'DPU Q4 2018'] = round(
                        product_dpu_18.loc[product_dpu_18['Quarter'] == 'Q4'][
                            'Measure Values'].values[0], 2)
                else:
                    scorecard.loc[index, 'DPU Q4 2018'] = \
                        product_dpu_18.loc[product_dpu_18['Quarter'] == 'Q4'][
                            'Measure Values'].values[0]

            # calculate dpu for 2019
            product_defect_size_19 = \
                product.loc[(product['Year'] == 2019) & (
                        product['Measure Names'] == 'Count of Defects')].groupby(
                    'Year')[
                    'Measure Values'].sum()
            product_route_total_size_19 = \
                product.loc[(product['Year'] == 2019) & (
                        product['Measure Names'] == 'Route_Total_Qty')].groupby(
                    'Year')[
                    'Measure Values'].sum()
            product_dpu_19 = (product_defect_size_19 / product_route_total_size_19).reset_index()
            product_dpu_19.fillna('inf', inplace=True)
            if len(product_dpu_19['Measure Values'].values) > 0:
                if product_dpu_19['Measure Values'].values[0] != '' and \
                        product_dpu_19['Measure Values'].values[
                            0] != 'inf':
                    scorecard.loc[index, 'DPU 2019 YTD'] = round(
                        product_dpu_19['Measure Values'].values[0], 2)
                else:
                    scorecard.loc[index, 'DPU 2019 YTD'] = product_dpu_19['Measure Values'].values[
                        0]
        elif row['Modality'] != "ALL" and row['Business'] != 'ALL' and row[
            'Product Segment'] == 'ALL':
            prod_seg = \
                data.loc[
                    (data['SC Business Segment'] == row['Business']) & (
                            data['SC Modality'] ==
                            row['Modality']) &
                    (data['SC Product Segment'] == row['Product Quality Scorecard'])]

            # calculate dpu for 2018
            prod_seg_defect_18 = \
                prod_seg.loc[
                    (prod_seg['Year'] == 2018) & (
                            prod_seg['Measure Names'] == 'Count of Defects')].groupby(
                    'Quarter')[
                    'Measure Values'].sum()

            prod_seg_route_total_size_18 = \
                prod_seg.loc[(prod_seg['Year'] == 2018) & (
                        prod_seg['Measure Names'] == 'Route_Total_Qty')].groupby(
                    'Quarter')[
                    'Measure Values'].sum()

            prod_seg_dpu_18 = (prod_seg_defect_18 / prod_seg_route_total_size_18).reset_index()
            prod_seg_dpu_18.fillna('inf', inplace=True)
            if len(prod_seg_dpu_18.loc[prod_seg_dpu_18['Quarter'] == 'Q3'][
                       'Measure Values'].values) > 0:
                if prod_seg_dpu_18.loc[prod_seg_dpu_18['Quarter'] == 'Q3'][
                    'Measure Values'].values[0] != '' and \
                        prod_seg_dpu_18.loc[prod_seg_dpu_18['Quarter'] == 'Q3'][
                            'Measure Values'].values[0] != 'inf':
                    scorecard.loc[index, 'DPU Q3 2018'] = round(
                        prod_seg_dpu_18.loc[prod_seg_dpu_18['Quarter'] == 'Q3'][
                            'Measure Values'].values[0], 2)
                else:
                    scorecard.loc[index, 'DPU Q3 2018'] = \
                        prod_seg_dpu_18.loc[prod_seg_dpu_18['Quarter'] == 'Q3'][
                            'Measure Values'].values[0]

            if len(prod_seg_dpu_18.loc[prod_seg_dpu_18['Quarter'] == 'Q4'][
                       'Measure Values'].values) > 0:
                if prod_seg_dpu_18.loc[prod_seg_dpu_18['Quarter'] == 'Q4'][
                    'Measure Values'].values[0] != '' and \
                        prod_seg_dpu_18.loc[prod_seg_dpu_18['Quarter'] == 'Q4'][
                            'Measure Values'].values[0] != 'inf':
                    scorecard.loc[index, 'DPU Q4 2018'] = round(
                        prod_seg_dpu_18.loc[prod_seg_dpu_18['Quarter'] == 'Q4'][
                            'Measure Values'].values[0], 2)
                else:
                    scorecard.loc[index, 'DPU Q4 2018'] = \
                        prod_seg_dpu_18.loc[prod_seg_dpu_18['Quarter'] == 'Q4'][
                            'Measure Values'].values[0]

            # calculate dpu for 2019
            prod_seg_defect_19 = \
                prod_seg.loc[
                    (prod_seg['Year'] == 2019) & (
                            prod_seg['Measure Names'] == 'Count of Defects')].groupby(
                    'Year')[
                    'Measure Values'].sum()
            prod_seg_route_total_size_19 = \
                prod_seg.loc[(prod_seg['Year'] == 2019) & (
                        prod_seg['Measure Names'] == 'Route_Total_Qty')].groupby(
                    'Year')[
                    'Measure Values'].sum()
            prod_seg_dpu_19 = (prod_seg_defect_19 / prod_seg_route_total_size_19).reset_index()
            prod_seg_dpu_19.fillna('inf', inplace=True)
            if len(prod_seg_dpu_19['Measure Values'].values) > 0:
                if prod_seg_dpu_19['Measure Values'].values[0] != '' and \
                        prod_seg_dpu_19['Measure Values'].values[
                            0] != 'inf':
                    scorecard.loc[index, 'DPU 2019 YTD'] = round(
                        prod_seg_dpu_19['Measure Values'].values[0], 2)
                else:
                    scorecard.loc[index, 'DPU 2019 YTD'] = prod_seg_dpu_19['Measure Values'].values[
                        0]
            else:
                scorecard.loc[index, 'DPU 2019 YTD'] = " "

    return scorecard


def combine_ydpu_sc(scorecard):
    scorecard.fillna('', inplace=True)
    for index, row in scorecard.iterrows():
        if row['Business'] != 'ALL' and row['Modality'] != 'ALL':
            if row['Yield Q3 2018'] != '' or row['DPU Q3 2018'] != '':
                scorecard.loc[index, 'Q3 2018'] = str(row['Yield Q3 2018']) + '%|' + str(
                    row['DPU Q3 2018'])
            if row['Yield Q4 2018'] != '' or row['DPU Q4 2018'] != '':
                scorecard.loc[index, 'Q4 2018'] = str(row['Yield Q4 2018']) + '%|' + str(
                    row['DPU Q4 2018'])
            if (row['Yield 2019 YTD'] != '' and row['Yield 2019 YTD'] != " " and
                row['Yield 2019 YTD'] != 'inf') or (row['DPU 2019 YTD'] != '' and
                                                    row['DPU 2019 YTD'] != " " and
                                                    row['DPU 2019 YTD'] != "inf"):
                scorecard.loc[index, '2019 YTD'] = str(row['Yield 2019 YTD']) + '%|' + str(
                    row['DPU 2019 YTD'])

    return scorecard


def get_modality_business_dpu(scorecard):
    scorecard['Yield 2019 YTD'].fillna(" ", inplace=True)
    scorecard['DPU 2019 YTD'].fillna(" ", inplace=True)
    modalities = \
        scorecard.loc[((scorecard['Business'] == 'IMAGING') & (scorecard['Modality'] == 'ALL'))][
            'Product Quality Scorecard'].unique()
    for modality in modalities:
        scorecard_rows = scorecard.loc[(
                (scorecard['Modality'] == modality) & (scorecard['Product Segment'] != 'ALL') & (
                scorecard['DPU 2019 YTD'] != '') & (scorecard['DPU 2019 YTD'] != 'inf'))]
        max_dpu_for_modality = scorecard_rows['DPU 2019 YTD'].max()
        max_index = scorecard_rows.index[scorecard_rows['DPU 2019 YTD'] == max_dpu_for_modality]
        max_product = scorecard_rows.loc[max_index]['Product Quality Scorecard']
        min_dpu_for_modality = scorecard_rows['DPU 2019 YTD'].min()
        min_index = scorecard_rows.index[scorecard_rows['DPU 2019 YTD'] == min_dpu_for_modality]
        min_product = scorecard_rows.loc[min_index]['Product Quality Scorecard']
        if len(min_product.values) > 0:
            min_prod = min_product.values[0]
        else:
            min_prod = " "
        if len(max_product.values) > 0:
            max_prod = max_product.values[0]
        else:
            max_prod = " "
        if max_prod == "NAN":
            max_prod = " "
        if min_prod == "NAN":
            min_prod = " "
        if max_prod != " " or min_prod != " ":
            scorecard.loc[((scorecard['Product Quality Scorecard'] == modality) & (
                    scorecard['Business'] == 'IMAGING') & (
                                   scorecard[
                                       'Modality'] == 'ALL')), 'Q3 2018'] = '2019' + ' ' + modality + ' ' + 'DPU - High: ' + \
                                                                            str(
                                                                                max_dpu_for_modality) + ' (' + str(
                max_prod) + ')' + ' Low: ' + str(min_dpu_for_modality) + ' (' + str(
                min_prod) + ')'
        else:
            scorecard.loc[((scorecard['Product Quality Scorecard'] == modality) & (
                    scorecard['Business'] == 'IMAGING') & (
                                   scorecard[
                                       'Modality'] == 'ALL')), 'Q3 2018'] = " "

    business_scorecard_rows = scorecard.loc[(
            (scorecard['Business'] == 'IMAGING') & (scorecard['Modality'] != 'ALL') & (
            scorecard['Product Segment'] != 'ALL') & (
                    scorecard['DPU 2019 YTD'] != '') & (scorecard['DPU 2019 YTD'] != 'inf'))]
    max_dpu_for_business = business_scorecard_rows['DPU 2019 YTD'].max()
    max_business_index = business_scorecard_rows.index[
        business_scorecard_rows['DPU 2019 YTD'] == max_dpu_for_business]
    max_product_business = business_scorecard_rows.loc[max_business_index][
        'Product Quality Scorecard']
    min_dpu_for_business = business_scorecard_rows['DPU 2019 YTD'].min()
    min_business_index = business_scorecard_rows.index[
        business_scorecard_rows['DPU 2019 YTD'] == min_dpu_for_business]
    min_product_business = business_scorecard_rows.loc[min_business_index][
        'Product Quality Scorecard']
    if len(min_product_business.values) > 0:
        min_prod = min_product_business.values[0]
    else:
        min_prod = " "
    if len(max_product_business.values) > 0:
        max_prod = max_product_business.values[0]
    else:
        max_prod = " "
    if max_prod == "NAN":
        max_prod = " "
    if min_prod == "NAN":
        min_prod = " "
    if max_prod != " " or min_prod != " ":
        scorecard.loc[((scorecard['Product Quality Scorecard'] == 'IMAGING') & (
                scorecard['Business'] == 'ALL') & (
                               scorecard[
                                   'Modality'] == 'ALL')), 'Q3 2018'] = '2019' + ' ' + 'IMAGING' + ' ' + 'DPU - High: ' + \
                                                                        str(
                                                                            max_dpu_for_business) + ' (' + str(
            max_prod) + ')' + ' Low: ' + str(min_dpu_for_business) + ' (' + str(
            min_prod) + ')'

    return scorecard


def get_modality_business_yield(scorecard):
    scorecard['Yield 2019 YTD'].fillna(" ", inplace=True)
    scorecard['DPU 2019 YTD'].fillna(" ", inplace=True)
    modalities = \
        scorecard.loc[((scorecard['Business'] == 'CCS') & (scorecard['Modality'] == 'ALL') & (
                scorecard['Product Quality Scorecard'] != 'LCS'))][
            'Product Quality Scorecard'].unique()
    for modality in modalities:
        scorecard_rows = scorecard.loc[(
                (scorecard['Modality'] == modality) & (scorecard['Product Segment'] != 'ALL') & (
                scorecard['Yield 2019 YTD'] != '') & (scorecard['Yield 2019 YTD'] != 'inf'))]
        max_yield_for_modality = scorecard_rows['Yield 2019 YTD'].max()
        max_index = scorecard_rows.index[scorecard_rows['Yield 2019 YTD'] == max_yield_for_modality]
        max_product = scorecard_rows.loc[max_index]['Product Quality Scorecard']
        min_yield_for_modality = scorecard_rows['Yield 2019 YTD'].min()
        min_index = scorecard_rows.index[scorecard_rows['Yield 2019 YTD'] == min_yield_for_modality]
        min_product = scorecard_rows.loc[min_index]['Product Quality Scorecard']
        if len(min_product.values) > 0:
            min_prod = min_product.values[0]
        else:
            min_prod = " "
        if len(max_product.values) > 0:
            max_prod = max_product.values[0]
        else:
            max_prod = " "
        scorecard.loc[((scorecard['Product Quality Scorecard'] == modality) & (
                scorecard['Business'] == 'CCS') & (
                               scorecard[
                                   'Modality'] == 'ALL')), 'Q3 2018'] = '2019' + ' ' + modality + ' ' + 'Yield - High: ' + \
                                                                        str(
                                                                            max_yield_for_modality) + '% (' + str(
            max_prod) + ')' + ' Low: ' + str(min_yield_for_modality) + '% (' + str(
            min_prod) + ')'

    # for LCS
    scorecard_rows = scorecard.loc[((scorecard['Business'] == 'CCS') &
                                    (scorecard['Modality'] != 'ULTRASOUND') & (
                                            scorecard['Modality'] != 'ALL') & (
                                            scorecard['Product Segment'] != 'ALL') & (
                                            scorecard['Yield 2019 YTD'] != '') & (
                                            scorecard['Yield 2019 YTD'] != 'inf'))]
    max_yield_for_modality = scorecard_rows['Yield 2019 YTD'].max()
    max_index = scorecard_rows.index[scorecard_rows['Yield 2019 YTD'] == max_yield_for_modality]
    max_product = scorecard_rows.loc[max_index]['Product Quality Scorecard']
    min_yield_for_modality = scorecard_rows['Yield 2019 YTD'].min()
    min_index = scorecard_rows.index[scorecard_rows['Yield 2019 YTD'] == min_yield_for_modality]
    min_product = scorecard_rows.loc[min_index]['Product Quality Scorecard']
    if len(min_product.values) > 0:
        min_prod = min_product.values[0]
    else:
        min_prod = " "
    if len(max_product.values) > 0:
        max_prod = max_product.values[0]
    else:
        max_prod = " "
    scorecard.loc[
        ((scorecard['Product Quality Scorecard'] == 'LCS') & (scorecard['Business'] == 'CCS') & (
                scorecard[
                    'Modality'] == 'ALL')), 'Q3 2018'] = '2019' + ' ' + 'LCS' + ' ' + 'Yield - High: ' + \
                                                         str(max_yield_for_modality) + '% (' + str(
        max_prod) + ')' + ' Low: ' + str(min_yield_for_modality) + '% (' + str(
        min_prod) + ')'

    # business
    business_scorecard_rows = scorecard.loc[(
            (scorecard['Business'] == 'CCS') & (scorecard['Modality'] != 'ALL') & (
            scorecard['Product Segment'] != 'ALL') & (
                    scorecard['Yield 2019 YTD'] != '') & (scorecard['Yield 2019 YTD'] != 'inf'))]
    max_yield_for_business = business_scorecard_rows['Yield 2019 YTD'].max()
    max_business_index = business_scorecard_rows.index[
        business_scorecard_rows['Yield 2019 YTD'] == max_yield_for_business]
    max_product_business = business_scorecard_rows.loc[max_business_index][
        'Product Quality Scorecard']
    min_yield_for_business = business_scorecard_rows['Yield 2019 YTD'].min()
    min_business_index = business_scorecard_rows.index[
        business_scorecard_rows['Yield 2019 YTD'] == min_yield_for_business]
    min_product_business = business_scorecard_rows.loc[min_business_index][
        'Product Quality Scorecard']
    if len(min_product_business.values) > 0:
        min_prod = min_product_business.values[0]
    else:
        min_prod = " "
    if len(max_product_business.values) > 0:
        max_prod = max_product_business.values[0]
    else:
        max_prod = " "
    scorecard.loc[
        ((scorecard['Product Quality Scorecard'] == 'CCS') & (scorecard['Business'] == 'ALL') & (
                scorecard[
                    'Modality'] == 'ALL')), 'Q3 2018'] = '2019' + ' ' + 'CCS' + ' ' + 'Yield - High: ' + \
                                                         str(max_yield_for_business) + '% (' + str(
        max_prod) + ')' + ' Low: ' + str(
        min_yield_for_business) + '% (' + str(
        min_prod) + ')'
    return scorecard


def setup_scrap_hierarchy(data, mapping):
    """
    Map scrap data from source to the scorecard hierarchy

    :param data: (Dataframe) scrap data from tableau source
    :param mapping: (Dataframe) modality mapping configuration
    :return: scrap data mapped to scorecard hierarchy (Dataframe)
    """
    data.rename(columns={"Calculated Modality": "SC Modality"}, inplace=True)
    data.rename(columns={"Modality.": "SC Modality"}, inplace=True)
    data["Total Scrap"] = data["Total Scrap"].str.replace(",", "").astype(
        float)
    time_periods = data["Time Axis"].unique()
    for time_period in time_periods:
        data.loc[data["Time Axis"] == time_period, "Year"] = \
            time_period.split("M")[0]
        data.loc[data["Time Axis"] == time_period, "Month"] = \
            time_period.split("M")[1]
    modalities = data["SC Modality"].unique()
    for modality in modalities:
        modality_data = mapping.loc[mapping["Modality"] == modality]
        if modality_data.size > 0:
            data.loc[data["SC Modality"] == modality, "SC Business Segment"] = \
                modality_data["Business for scorecard"] \
                    .values[0]
        else:
            data.loc[data["SC Modality"] == modality, "SC Business Segment"] = "GEHC OTHER"
    data["Year"] = data["Year"].astype(int)
    data["Month"] = data["Month"].astype(int)

    data = data.loc[(data["Year"] < current_year) | ((data["Year"] == current_year)
                                                     & (data["Month"] <= (current_month)))]
    return data


def calculate_scrap(data, score_card, metric):
    """
    Calculate scrap metric for the scorecard
    :param data: (Dataframe) scrap data from tableau source
    :param score_card:  (Dataframe) scorecard structure
    :return: scorecard (Dataframe) with values
    """

    for index, row in score_card.iterrows():
        if scorecard_functions.is_gehc_level(row):
            gehc = data.groupby("Year")["Total Scrap"].sum()
            score_card = scorecard_functions.set_metric_value_current(gehc, index, score_card)
            gehc_ytd = scorecard_functions.get_previous_year_data(data, metric)
            gehc_ytd_group = gehc_ytd.groupby("Year")["Total Scrap"].sum()
            score_card = scorecard_functions.set_metric_value_previous(gehc_ytd_group, index,
                                                                       score_card)

        elif scorecard_functions.is_business_level(row):
            business = scorecard_functions.get_business_data(data, row, False, 0, metric)
            business_group = business.groupby("Year")["Total Scrap"].sum()
            score_card = scorecard_functions.set_metric_value_current(business_group, index,
                                                                      score_card)
            business_ytd = scorecard_functions.get_business_data(data, row, True, 0, metric)
            business_ytd_group = business_ytd.groupby("Year")["Total Scrap"].sum()
            score_card = scorecard_functions.set_metric_value_previous(business_ytd_group, index,
                                                                       score_card)
        else:
            modality = scorecard_functions.get_modality_data(data, row, False, False, 0, metric)
            modality_group = modality.groupby("Year")["Total Scrap"].sum()
            score_card = scorecard_functions.set_metric_value_current(modality_group, index,
                                                                      score_card)
            modality_ytd = scorecard_functions.get_modality_data(data, row, False, True, 0, metric)
            modality_ytd_group = modality_ytd.groupby("Year")["Total Scrap"].sum()
            if modality_ytd_group.size > 0:
                score_card = scorecard_functions.set_metric_value_previous(modality_ytd_group,
                                                                           index,
                                                                           score_card)
    return score_card


def setup_concessions_hierarchy(data, mapping):
    """
        Calculate concessions metric for the scorecard
        :param data: (Dataframe) concessions data from manual upload to git
        :param mapping:  (Dataframe) modality mapping configuration
        :return: concessions data mapped to scorecard hierarchy (Dataframe)
        """
    quarters = data["Approved Year and Quarter"].unique()
    for quarter in quarters:
        data.loc[data["Approved Year and Quarter"] == quarter, "Quarter"] = quarter.split("-")[1]
        data.loc[data["Approved Year and Quarter"] == quarter, "Year"] = quarter.split("-")[0]
    quarter_values = data["Quarter"].unique()
    for quarter_value in quarter_values:
        data.loc[data["Quarter"] == quarter_value, "Quarter Number"] = quarter_value[-1:]
    data["Year"] = data["Year"].astype(int)
    data = data.loc[data["Year"] >= 2016]
    modality_values = data["Modality"].unique()
    for modality_value in modality_values:
        modality_data = mapping.loc[mapping["Modality"] == modality_value]
        if modality_data.size > 0:
            data.loc[data["Modality"] == modality_value, "SC Business Segment"] = modality_data[
                "SC Business"].values[0]
            data.loc[data["Modality"] == modality_value, "SC Modality"] = modality_data[
                "SC Modality"].values[0]
        else:
            data.loc[data["Modality"] == modality_value, "SC Business Segment"] = "GEHC OTHER"
            data.loc[data["Modality"] == modality_value, "SC Modality"] = "OTHER"
    return data


def calculate_concessions(score_card, data, metric):
    """
        Calculate concessions metric for the scorecard
        :param data: (Dataframe) concessions data from manual upload to git
        :param score_card:  (Dataframe) scorecard structure
        :return: scorecard (Dataframe) with values
        """
    data["FMV $"] = scorecard_functions.remove_comma_from_df_column(data["FMV $"])
    for index, row in score_card.iterrows():
        if scorecard_functions.is_gehc_level(row):
            gehc = data.groupby("Year")["FMV $"].sum()
            score_card = scorecard_functions.set_metric_value_current(gehc, index, score_card)

        elif scorecard_functions.is_business_level(row):
            business = scorecard_functions.get_business_data(data, row, False, 0, metric)
            business_group = business.groupby("Year")["FMV $"].sum()
            score_card = scorecard_functions.set_metric_value_current(business_group, index,
                                                                      score_card)

        else:
            modality = scorecard_functions.get_modality_data(data, row, False, False, 0, metric)
            modality_group = modality.groupby("Year")["FMV $"].sum()
            score_card = scorecard_functions.set_metric_value_current(modality_group, index,
                                                                      score_card)

    score_card.drop(columns=[str(current_year - 1) + " YTD"], inplace=True)

    return score_card


def setup_new_chu_hierarchy(data, cr_hierarchy):
    """
    map complaint rate to chu hierarchy. This is the new hierarchy whic h does not have
    product segment
    :param data:
    :param cr_hierarchy:
    :return:
    """
    time_axes = data["Quarter"].unique()
    for time in time_axes:
        data.loc[data['Quarter'] == time, 'Year'] = time[-4:]
        data.loc[data['Quarter'] == time, 'Quarter'] = time[1:2]
    data["Year"] = data["Year"].astype(int)
    data["Quarter"] = data["Quarter"].astype(int)
    data = scorecard_functions.set_business(data, cr_hierarchy)
    data.rename(
        columns={"Modality": "SC Modality", "Product Group / Modality Segment - IB": "SC Product"},
        inplace=True)
    data["SC Product"].fillna("<BLANK IN SOURCE>", inplace=True)
    return data

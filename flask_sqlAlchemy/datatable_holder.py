import datetime as dt
import dateutil
import os
import pandas as pd
import numpy as np
import math
import copy
from common import Common, Price

com = Common()
def holder(cnxn):
    pricing_securities = pricing_create_datatable(cnxn)

    pricing_securities = pricing_filter_df(pricing_securities)
    pricing_securities = pricing_format_filtered_df(pricing_securities, 'classic')
    return pricing_securities

def pricing_create_datatable(cnxn):
    date_tday = (dt.date.today()).strftime("%Y-%m-%d")
    fname = "M:\\Big_Data\\Txt_Files\\pricing_starter_table_" + date_tday + ".txt"

    if os.path.isfile(fname) and com.test_page != 'pricing':
        mydf = pd.read_csv(fname, sep=",")
        mydf.set_index('CUSIP', inplace=True)
        return mydf
    else:
        date_yday = (pd.read_sql_query(
            "SELECT MAX(DATEOF) FROM PORTFOLIO_MASTER_SECURITIES WHERE DATEOF < CONVERT(DATETIME, '" + (
                dt.date.today()).strftime("%Y-%m-%d") + " 00:00:00', 102)", cnxn)).iloc[-1, -1]

        # get yesterday holdings
        query = "SELECT CUSIP, BBG, SECURITY_NAME, TICKER, CORP_TICKER, CURRENCY AS 'CUR', COUNTRY, RATING, " \
                "ASSET_CLASS, CLASSIFICATION AS 'CLASS', INDUSTRY, INDUSTRY_CLEAN, YEARS_WORST AS 'YEARSW', " \
                "YIELD_WORST, DURATION, CURRENT_YIELD AS 'CY', RANK AS 'RANK', ISSUE_DATE, ISIN " \
                "FROM PORTFOLIO_MASTER_SECURITIES WHERE DATEOF = CONVERT(DATETIME, '" + date_yday + \
                " 00:00:00', 102) AND ILLIQUID != 'IL'"

        tmp = "SELECT DISTINCT DATEOF FROM PORTFOLIO_MASTER_PRICING WHERE DATEOF >= DATEADD(WEEK, -5, GETDATE()) AND " \
              "DATEOF < GETDATE() ORDER BY DATEOF"
        movers_days = pd.read_sql_query(tmp, cnxn)
        total_days_list = movers_days['DATEOF'].values.tolist()
        total_days_list = [dt.datetime.strptime(date, '%Y-%m-%d').date() for date in total_days_list]

        def nearest(date_list, pivot):
            return min(date_list, key=lambda  sub: abs(sub - pivot))

        prev_1m = dt.date.today() - dateutil.relativedelta.relativedelta(months=(1))
        movers_date1m = nearest(total_days_list, prev_1m).strftime('%Y-%m-%d')
        prev_7d = dt.date.today() - dateutil.relativedelta.relativedelta(days=(7))
        movers_date1w = nearest(total_days_list, prev_7d).strftime('%Y-%m-%d')

        df = pd.read_sql_query(query, cnxn)
        df['CUSIP_CODE'] = df['CUSIP']
        df['CUSIP_INDEX'] = df['CUSIP']
        df.set_index('CUSIP', inplace=True)
        df["BBG"] = df["BBG"].str.upper()
        #df["BBG"] = df["BBG"].replace('CN EQUITY', 'CT EQUITY', regex=True)
        df["BBG"] = df["BBG"].replace(' GOVT', '@BGN GOVT', regex=True)
        df["BBG"] = df["BBG"].replace(' CORP', '@MSG1 CORP', regex=True)
        df["BBG"] = df["BBG"].replace('CBIN@MSG1 CORP', 'CBIN CORP', regex=True)
        df["BBG"] = df["BBG"].replace(' @MSG1 CORP', '@MSG1 CORP', regex=True)
        df["BBG"] = df["BBG"].replace('BVAL', 'MSG1', regex=True)

        # clean industries
        df['IND'] = np.nan
        def ind_cleaner_pricing(ind, asset):
            if (asset in ['HG', 'HY', 'LN', 'CV', 'AB', 'CP', 'EQ', 'CP']):
                if str(ind).upper() in ['NONE', 'NAN', 'NULL', 'NAT', "'NONE'", "'NAN'", "'NULL'", "'NAT'"]:
                    return 'OTHER'
                else:
                    return str(ind).upper()
            else:
                return np.nan
        df['IND'] = df.apply(lambda x: ind_cleaner_pricing(x.INDUSTRY_CLEAN, x.ASSET_CLASS), axis=1)

        # clean rank
        def rank_cleaner_pricing(rank, asset):
            if (asset in ['HG', 'HY']):
                if str(rank).upper() in ['NONE', 'NAN', 'NULL', 'NAT', "'NONE'", "'NAN'", "'NULL'", "'NAT'"]:
                    return np.nan
                else:
                    try:
                        rank_lookup_dict = {'ASSET BACKED': 'Secured',
                                            '1ST LIEN': 'Secured',
                                            '2ND LIEN': 'Secured',
                                            'SECURED': 'Secured',
                                            'SR UNSECURED': 'Unsecured',
                                            'UNSECURED': 'Unsecured',
                                            'SUBORDINATED': 'Unsecured',
                                            'SR SUBORDINATED': 'Unsecured',
                                            'JR SUBORDINATED': 'Unsecured',
                                            'SR NON PREFERRED': 'Unsecured'}
                        new_rank = str(rank_lookup_dict[str(rank).upper()])
                        return new_rank
                    except:
                        return np.nan
            elif asset in ['AB', 'LN']:
                return 'Secured'
            else:
                return np.nan
        df['RANK'] = df.apply(lambda x: rank_cleaner_pricing(x.RANK, x.ASSET_CLASS), axis=1)

        # fill cyc and tags
        df_maxdate_ov = pd.read_sql_query("SELECT MAX(DATEOF) FROM PORTFOLIO_MASTER_PRICING", cnxn)
        if ((df_maxdate_ov.iloc[-1, -1] is None) or (df_maxdate_ov.empty)):
            max_date_stored_ov = "nan"
        else:
            max_date_stored_ov = df_maxdate_ov.iloc[-1, -1]
        df['CYC'] = np.nan
        df['TAG'] = np.nan
        if max_date_stored_ov != "nan":
            df_over_ov = pd.read_sql_query("SELECT CUSIP, CYC AS 'CYC_OV', TAG AS 'TAG_OV' FROM "
                                           "PORTFOLIO_MASTER_PRICING WHERE DATEOF = CONVERT(DATETIME, '"
                                           + max_date_stored_ov + " 00:00:00', 102)", cnxn)
            df_over_ov['CUSIP_CODE'] = df_over_ov['CUSIP']
            df_over_ov.set_index('CUSIP', inplace=True)
            df = df.merge(df_over_ov, on='CUSIP_CODE', how='left')
            df.loc[df['CYC_OV'].notnull(), 'CYC'] = df['CYC_OV']#.notnull()
            df.loc[df['TAG_OV'].notnull(), 'TAG'] = df['TAG_OV']
            df = df.drop(['CYC_OV', 'TAG_OV'], axis=1)
            df['CUSIP'] = df['CUSIP_CODE']
            df.set_index('CUSIP', inplace=True)

        # create and format required columns
        df.loc[df['ASSET_CLASS'] == 'EQ', 'CLASS'] = 'EQ'
        df.loc[df['ASSET_CLASS'] == 'SU', 'CLASS'] = 'SU'
        df.loc[df['ASSET_CLASS'] == 'LN', 'CLASS'] = 'LN'
        df.loc[df['ASSET_CLASS'] == 'AB', 'CLASS'] = 'OTHER'
        df.loc[df['ASSET_CLASS'] == 'CV', 'CLASS'] = 'OTHER'
        df.loc[df['ASSET_CLASS'] == 'CP', 'CLASS'] = 'OTHER'
        df.loc[df['ASSET_CLASS'] == 'EQ', 'YEARSW'] = np.nan
        df.loc[df['ASSET_CLASS'] == 'OTHER', 'YEARSW'] = np.nan
        df.loc[df['ASSET_CLASS'] == 'OTHER', 'CLASS'] = 'OTHER'
        df['YIELD_WORST'] = df['YIELD_WORST'].astype(float)
        df.loc[df['ASSET_CLASS'] == 'EQ', 'YIELD_WORST'] = np.nan
        df.loc[df['ASSET_CLASS'] == 'OTHER', 'YIELD_WORST'] = np.nan
        df['YEARSW'] = df['YEARSW'].round(2)
        df['YIELD_WORST'] = df['YIELD_WORST'].round(2)
        df['CLASS'] = df['CLASS'].map(lambda x: pricing_clean_classification(x))
        df['SPREAD_CHG'] = np.nan
        df['PRICE_CHG'] = np.nan
        df['DOLLAR_CHG'] = np.nan
        df['SPREAD_FINAL_YEST'] = np.nan
        df['SPREAD_FINAL_WEEK'] = np.nan
        df['SPREAD_FINAL_MNTH'] = np.nan
        df['PRICE_FINAL_YEST'] = np.nan
        df['PRICE_FINAL_WEEK'] = np.nan
        df['PRICE_FINAL_MNTH'] = np.nan
        df['PRICE_INIT_YEST'] = np.nan
        df['SPREAD_BBG_YEST'] = np.nan
        df['PRICE_BBG_YEST'] = np.nan
        df['SPREAD_BBG_WEEK'] = np.nan
        df['PRICE_BBG_WEEK'] = np.nan
        df['SPREAD_BBG_MNTH'] = np.nan
        df['PRICE_BBG_MNTH'] = np.nan
        df['SPREAD_BBG'] = np.nan
        df['PRICE_BBG'] = np.nan
        df['BENCH_BBG'] = np.nan
        df['BENCHNAME_BBG'] = np.nan
        df['SPREAD_FINAL'] = np.nan
        df['PRICE_FINAL'] = np.nan
        df['BENCH_FINAL'] = np.nan
        df['BENCHNAME_FINAL'] = np.nan
        df['SPREAD_OV'] = np.nan
        df['PRICE_OV'] = np.nan
        df['PRICE_OV_YEST'] = np.nan
        df['BENCH_OV'] = np.nan
        df['SOURCE_OV'] = np.nan
        df['NEWI_OV'] = np.nan
        df['ANY_OV'] = np.nan
        df['ANY_SPREAD_OV'] = np.nan
        df['BBG_CLEAN'] = df['BBG']
        df['BBG_CLEAN'] = df['BBG_CLEAN'].str.replace("@BGN", "")
        df['BBG_CLEAN'] = df['BBG_CLEAN'].str.replace("@MSG1", "")
        df['BBG_CLEAN'] = df['BBG_CLEAN'].str.replace("@BVAL", "")
        df['BBG_CLEAN'] = df['BBG_CLEAN'].str.replace("@", "")
        df['BBG_CLEAN'] = df['BBG_CLEAN'].str.replace("  ", " ")

        # get wt, qty, ctr
        accts_string = ""
        for account in Price.pricing_accounts:
            accts_string = accts_string + ", '" + account + "'"
        accts_string = accts_string[2:]
        accts_string = accts_string + ", '173'"
        query = "SELECT CUSIP, ACCOUNT, WEIGHT, QUANTITY, MARKET_VALUE FROM PORTFOLIO_MASTER_ALLOCATIONS WHERE DATEOF" \
                " = CONVERT(DATETIME, '" + date_yday + " 00:00:00', 102) AND ACCOUNT IN (" + accts_string + ")"
        df_holdings = pd.read_sql_query(query, cnxn)
        df_wts = copy.deepcopy(df_holdings)
        df_wts['CUSIP_ACCOUNT'] = df_wts['CUSIP'] + "_" + df_wts['ACCOUNT']
        df_wts['ACCOUNT'] = df_wts['ACCOUNT'] + "_Wt"
        df_wts['WEIGHT'] = df_wts['WEIGHT'] * 100.0
        df_wts.set_index('CUSIP_ACCOUNT', inplace=True)
        df_wts = df_wts.pivot_table(index=['CUSIP'], columns=['ACCOUNT'], values='WEIGHT')
        df = pd.concat([df, df_wts], axis=1)
        df.columns = df.columns.map(str)
        df = df[pd.notnull(df['SECURITY_NAME'])]
        df_qty = copy.deepcopy(df_holdings)
        df_qty['CUSIP_ACCOUNT'] = df_qty['CUSIP'] + "_" + df_qty['ACCOUNT']
        df_qty['ACCOUNT'] = df_qty['ACCOUNT'] + "_Qty"
        df_qty.set_index('CUSIP_ACCOUNT', inplace=True)
        df_qty = df_qty.pivot_table(index=['CUSIP'], columns=['ACCOUNT'], values='QUANTITY')
        df = pd.concat([df, df_qty], axis=1)
        df.columns = df.columns.map(str)
        df = df[pd.notnull(df['SECURITY_NAME'])]
        df_bad = copy.deepcopy(df)
        df['Total_Qty'] = 0.0
        tmp_account = Price.pricing_accounts + ['173']
        for account in tmp_account:
            df['Total_Qty'] = df['Total_Qty'] + df[account + '_Qty'].fillna(0.0)
            net = df[account + '_Wt'].sum()
            df[account + '_Wt'] = df[account + '_Wt'] * 100.0 / net
            df[account + '_R'] = (df[account + '_Wt'].astype(float) / 100.0) * df['PRICE_CHG'].astype(float)
            df[account + '_CTD'] = (df[account + '_Wt'].astype(float) / 100.0) * df['DURATION'].astype(float)

        # remove securities not currently held
        df_bad = df_bad[df_bad.filter(like='_Wt').isnull().all(1)]
        del_list = list(df_bad['CUSIP_CODE'])
        #del_list.remove('912810SF6') # for 173
        df = df[~df['CUSIP_CODE'].isin(del_list)]

        # get total data for securities stored
        df_total = pd.read_sql_query("SELECT CUSIP, PRICE_FINAL, SPREAD_FINAL, DATEOF"
                                     " FROM PORTFOLIO_MASTER_PRICING WHERE DATEOF "
                                    "in (CONVERT(DATETIME, '" + date_yday + " 00:00:00', 102),"
                                    "CONVERT(DATETIME, '" + movers_date1w + " 00:00:00', 102), "
                                    "CONVERT(DATETIME, '" + movers_date1m + " 00:00:00', 102))", cnxn)
        df_total['CUSIP_CODE'] = df_total['CUSIP']
        df_total.set_index('CUSIP', inplace=True)
        df_yest = df_total.loc[df_total['DATEOF'] == date_yday]
        df_yest = df_yest.drop(['DATEOF'], axis=1)
        df_yest = df_yest.rename(columns={'PRICE_FINAL': 'PRICE_FINAL_YEST1', 'SPREAD_FINAL': 'SPREAD_FINAL_YEST1'})

        df_1w = df_total.loc[df_total['DATEOF'] == movers_date1w]
        df_1w = df_1w.drop(['DATEOF'], axis=1)
        df_1w = df_1w.rename(columns={'PRICE_FINAL': 'PRICE_FINAL_WEEK1', 'SPREAD_FINAL': 'SPREAD_FINAL_WEEK1'})

        df_1m = df_total.loc[df_total['DATEOF'] == movers_date1m]
        df_1m = df_1m.drop(['DATEOF'], axis=1)
        df_1m = df_1m.rename(columns={'PRICE_FINAL': 'PRICE_FINAL_MNTH1', 'SPREAD_FINAL': 'SPREAD_FINAL_MNTH1'})

        df['PRICE_FINAL_YEST'] = np.nan
        df['SPREAD_FINAL_YEST'] = np.nan
        df = df.merge(df_yest, on='CUSIP_CODE', how='left')

        df['PRICE_FINAL_WEEK'] = np.nan
        df['SPREAD_FINAL_WEEK'] = np.nan
        df = df.merge(df_1w, on='CUSIP_CODE', how='left')

        df['PRICE_FINAL_MNTH'] = np.nan
        df['SPREAD_FINAL_MNTH'] = np.nan
        df = df.merge(df_1m, on='CUSIP_CODE', how='left')
        df_yest.to_csv("M:\\Big_Data\\Txt_Files\\df_yest.txt")
        df_1w.to_csv("M:\\Big_Data\\Txt_Files\\df_1w.txt")
        df_1m.to_csv("M:\\Big_Data\\Txt_Files\\df_1m.txt")

        df.loc[df['PRICE_FINAL_YEST'].isnull(), 'PRICE_FINAL_YEST'] = df['PRICE_FINAL_YEST1']
        df.loc[df['SPREAD_FINAL_YEST'].isnull(), 'SPREAD_FINAL_YEST'] = df['SPREAD_FINAL_YEST1']
        df.loc[df['PRICE_FINAL_WEEK'].isnull(), 'PRICE_FINAL_WEEK'] = df['PRICE_FINAL_WEEK1']
        df.loc[df['SPREAD_FINAL_WEEK'].isnull(), 'SPREAD_FINAL_WEEK'] = df['SPREAD_FINAL_WEEK1']
        df.loc[df['PRICE_FINAL_MNTH'].isnull(), 'PRICE_FINAL_MNTH'] = df['PRICE_FINAL_MNTH1']
        df.loc[df['SPREAD_FINAL_MNTH'].isnull(), 'SPREAD_FINAL_MNTH'] = df['SPREAD_FINAL_MNTH1']
        df = df.drop(['PRICE_FINAL_YEST1', 'SPREAD_FINAL_YEST1', 'PRICE_FINAL_WEEK1', 'SPREAD_FINAL_WEEK1', 'PRICE_FINAL_MNTH1', 'SPREAD_FINAL_MNTH1'], axis=1)
        df['CUSIP'] = df['CUSIP_CODE']
        df.set_index('CUSIP', inplace=True)
        df.loc[~df['ASSET_CLASS'].isin(['SU', 'LN', 'HY', 'HG', 'GV', 'EQ', 'AB', 'CV', 'CP', 'OTHER']), 'PRICE_FINAL'] = 0.0
        df.loc[~df['ASSET_CLASS'].isin(['SU', 'LN', 'HY', 'HG', 'GV', 'EQ', 'AB', 'CV', 'CP', 'OTHER']), 'PRICE_FINAL_YEST'] = 0.0

        # get yesterday data for securities not stored
       # df_yest_missing_p = copy.deepcopy(df)
       # df_yest_missing_p = df_yest_missing_p.loc[df_yest_missing_p['PRICE_FINAL_YEST'].isnull()]
       # df_week_missing_p = copy.deepcopy(df)
      #  df_week_missing_p = df_week_missing_p.loc[df_week_missing_p['PRICE_FINAL_WEEK'].isnull()]
      #  df_mnth_missing_p = copy.deepcopy(df)
       # df_mnth_missing_p = df_mnth_missing_p.loc[df_mnth_missing_p['PRICE_FINAL_MNTH'].isnull()]
        # q = 0
        # # print('getting missing yest: '+str(len(df_yest_missing_p.index)))
        # for index, row in df_yest_missing_p.iterrows():
        #     q = q + 1
        #     bbg = str(row['BBG'])
        #     bbg = bbg.replace("@BGN", "")
        #     bbg = bbg.replace("@MSG1", "")
        #     bbg = bbg.replace("@BVAL", "")
        #     bbg = bbg.replace("@", "")
        #     bbg = bbg.replace("  ", " ")
        #     if (str(row['ASSET_CLASS']) in ['HY', 'EQ', 'AB', 'CV', 'LN', 'OTHER']) or ('CXPHY' in bbg):
        #         p = master_settings.grab_spot(bbg, 'PX_LAST', mydate=master_settings.date_yday)
        #         if str(p) != 'nan':
        #             df.loc[df['BBG_CLEAN'] == bbg, 'PRICE_BBG_YEST'] = p
        #         else:
        #             df.loc[df['BBG_CLEAN'] == bbg, 'PRICE_BBG_YEST'] = master_settings.grab_spot(bbg, 'PX_LAST')
        #     elif str(row['ASSET_CLASS']) == 'GV':
        #         p = master_settings.grab_spot(bbg, 'PX_LAST', mydate=master_settings.date_yday)
        #         f = master_settings.grab_spot(bbg, 'PRINCIPAL_FACTOR')
        #         if str(p) != "nan" and str(f) != "nan":
        #              df.loc[df['BBG_CLEAN'] == bbg, 'PRICE_BBG_YEST'] = float(p) * float(f)
        #         elif str(p) != "nan":
        #              df.loc[df['BBG_CLEAN'] == bbg, 'PRICE_BBG_YEST'] = float(p)
        #         elif str(f) != "nan":
        #             p2 = master_settings.grab_spot(bbg, 'PX_LAST')
        #             if str(p2) != "nan":
        #                 df.loc[df['BBG_CLEAN'] == bbg, 'PRICE_BBG_YEST'] = float(p2) * float(f)
        #         else:
        #             df.loc[df['BBG_CLEAN'] == bbg, 'PRICE_BBG_YEST'] = master_settings.grab_spot(bbg, 'PX_LAST')
        #     elif str(row['ASSET_CLASS']) == 'HG':
        #         p = master_settings.grab_spot(bbg, 'PX_LAST', mydate=master_settings.date_yday)
        #         if str(p) != 'nan':
        #             df.loc[df['BBG_CLEAN'] == bbg, 'PRICE_BBG_YEST'] = float(p)
        #         else:
        #             df.loc[df['BBG_CLEAN'] == bbg, 'PRICE_BBG_YEST'] = master_settings.grab_spot(bbg, 'PX_LAST')
        #         s = master_settings.grab_spot(bbg, 'BLP_SPRD_TO_BENCH_MID', mydate=master_settings.date_yday)
        #         if str(s) != 'nan':
        #             df.loc[df['BBG_CLEAN'] == bbg, 'SPREAD_BBG_YEST'] = float(s)
        #         else:
        #             df.loc[df['BBG_CLEAN'] == bbg, 'SPREAD_BBG_YEST'] = master_settings.grab_spot(bbg, 'BLP_SPRD_TO_BENCH_MID')
        #     elif ('ITXEX' in bbg) or ('CDXIG' in bbg):
        #         df.loc[df['BBG_CLEAN'] == bbg, 'SPREAD_BBG_YEST'] = master_settings.grab_spot(bbg, 'PX_LAST', mydate=master_settings.date_yday)
        # q = 0
        # # print('getting missing week: '+str(len(df_week_missing_p.index)))
        # for index, row in df_week_missing_p.iterrows():
        #     q = q + 1
        #     bbg = str(row['BBG'])
        #     bbg = bbg.replace("@BGN", "")
        #     bbg = bbg.replace("@MSG1", "")
        #     bbg = bbg.replace("@BVAL", "")
        #     bbg = bbg.replace("@", "")
        #     bbg = bbg.replace("  ", " ")
        #     if str(row['ASSET_CLASS']) == 'HY':
        #         p = master_settings.grab_spot(bbg, 'PX_LAST', mydate=movers_date1w)
        #         if str(p) != 'nan':
        #             df.loc[df['BBG_CLEAN'] == bbg, 'PRICE_BBG_WEEK'] = float(p)
        #         else:
        #             p = master_settings.grab_spot(bbg, 'ISSUE_PX')
        #             if str(p) != 'nan':
        #                 df.loc[df['BBG_CLEAN'] == bbg, 'PRICE_BBG_WEEK'] = float(p)
        #             else:
        #                 df.loc[df['BBG_CLEAN'] == bbg, 'PRICE_BBG_WEEK'] = 100.0
        #     elif str(row['ASSET_CLASS']) == 'HG':
        #         p = master_settings.grab_spot(bbg, 'PX_LAST', mydate=movers_date1w)
        #         if str(p) != 'nan':
        #             df.loc[df['BBG_CLEAN'] == bbg, 'PRICE_BBG_WEEK'] = float(p)
        #         else:
        #             p = master_settings.grab_spot(bbg, 'ISSUE_PX')
        #             if str(p) != 'nan':
        #                 df.loc[df['BBG_CLEAN'] == bbg, 'PRICE_BBG_WEEK'] = float(p)
        #             else:
        #                 df.loc[df['BBG_CLEAN'] == bbg, 'PRICE_BBG_WEEK'] = 100.0
        #         s = master_settings.grab_spot(bbg, 'BLP_SPRD_TO_BENCH_MID', mydate=movers_date1w)
        #         if str(s) != 'nan':
        #             df.loc[df['BBG_CLEAN'] == bbg, 'SPREAD_BBG_WEEK'] = float(s)
        #         else:
        #             s = master_settings.grab_spot(bbg, 'ISSUE_SPREAD_BNCHMRK')
        #             if str(s) != 'nan':
        #                 df.loc[df['BBG_CLEAN'] == bbg, 'SPREAD_BBG_WEEK'] = float(s)
        # q = 0
        # # print('getting missing mnth: '+str(len(df_mnth_missing_p.index)))
        # for index, row in df_mnth_missing_p.iterrows():
        #     q = q + 1
        #     bbg = str(row['BBG'])
        #     bbg = bbg.replace("@BGN", "")
        #     bbg = bbg.replace("@MSG1", "")
        #     bbg = bbg.replace("@BVAL", "")
        #     bbg = bbg.replace("@", "")
        #     bbg = bbg.replace("  ", " ")
        #     if str(row['ASSET_CLASS']) == 'HY':
        #         p = master_settings.grab_spot(bbg, 'PX_LAST', mydate=movers_date1m)
        #         if str(p) != 'nan':
        #             df.loc[df['BBG_CLEAN'] == bbg, 'PRICE_BBG_MNTH'] = float(p)
        #     elif str(row['ASSET_CLASS']) == 'HG':
        #         p = master_settings.grab_spot(bbg, 'PX_LAST', mydate=movers_date1m)
        #         if str(p) != 'nan':
        #             df.loc[df['BBG_CLEAN'] == bbg, 'PRICE_BBG_MNTH'] = float(p)
        #         s = master_settings.grab_spot(bbg, 'BLP_SPRD_TO_BENCH_MID', mydate=movers_date1m)
        #         if str(s) != 'nan':
        #             df.loc[df['BBG_CLEAN'] == bbg, 'SPREAD_BBG_MNTH'] = float(s)
        #         else:
        #             s = master_settings.grab_spot(bbg, 'ISSUE_SPREAD_BNCHMRK')
        #             if str(s) != 'nan':
        #                 df.loc[df['BBG_CLEAN'] == bbg, 'SPREAD_BBG_MNTH'] = float(s)
        # print('done filling missing')
        df.loc[df['PRICE_FINAL_YEST'].isnull(), 'PRICE_FINAL_YEST'] = df['PRICE_BBG_YEST']
        df.loc[df['SPREAD_FINAL_YEST'].isnull(), 'SPREAD_FINAL_YEST'] = df['SPREAD_BBG_YEST']
        df.loc[df['PRICE_FINAL_WEEK'].isnull(), 'PRICE_FINAL_WEEK'] = df['PRICE_BBG_WEEK']
        df.loc[df['SPREAD_FINAL_WEEK'].isnull(), 'SPREAD_FINAL_WEEK'] = df['SPREAD_BBG_WEEK']
        df.loc[df['PRICE_FINAL_MNTH'].isnull(), 'PRICE_FINAL_MNTH'] = df['PRICE_BBG_MNTH']
        df.loc[df['SPREAD_FINAL_MNTH'].isnull(), 'SPREAD_FINAL_MNTH'] = df['SPREAD_BBG_MNTH']

        df['PRICE_INIT_YEST'] = df['PRICE_FINAL_YEST']
        df = df.drop(['BBG_CLEAN'], axis=1)
        df['CUSIP'] = df['CUSIP_CODE']
        df.set_index('CUSIP', inplace=True)
        df = pricing_format_df(df)
        df['BBG_DROPDOWN'] = np.nan

        df['BENCH_BBG'] = np.nan
        df['BENCHNAME_BBG'] = np.nan
        df['BENCH_BBG_TEMP'] = np.nan
        df['BENCHNAME_TEMP'] = np.nan
        df['BENCH_RATIO'] = np.nan
        df['BENCH_PX_YES'] = np.nan
        df['BENCH_PX_NOW'] = np.nan
        #df_hg = copy.deepcopy(df)
        #df_hg = df_hg[df_hg['ASSET_CLASS']=='HG']

        df.to_csv("M:\\Big_Data\\Txt_Files\\pricing_starter_table_" + date_tday+".txt")
        return df

def pricing_format_filtered_df(df, sortby):
    def stringer(x, d):
        try:
            if math.isnan(float(x)) or float(x) == 0.0:
                return np.nan
            elif d == 0:
                return str(int(round(float(x), 0)))
            elif d in [1, 2, 3]:
                if round(float(x), d) == 0.0:
                    return np.nan
                else:
                    return '{:.{i}f'.format(float(x), i=d) # format to a certain float
            else:
                return str(x)
        except:
            return np.nan

    def sort_asset(x):
        if x == 'SU':
            return 1
        elif x == 'LN':
            return 2
        elif x == 'HY':
            return 3
        elif x == 'HG':
            return 4
        elif x == 'GV':
            return 5
        elif x == 'AB':
            return 6
        elif x == 'CV':
            return 7
        elif x == 'EQ':
            return 8
        elif x == 'BT':
            return 9
        else:
            return 10

    def sort_industry(x):
        if x != 'Futures':
            return 1
        else:
            return 2

    if sortby == "chg":
        df = df.sort_values(by=['PRICE_CHG'], ascending=[False])
    elif sortby == "qty" and (1 in Price.pricing_accounts_sel):
        my_accts = [(b + '_Qty') for a, b in zip(Price.pricing_accounts_sel, Price.pricing_accounts) if a != 0]
        if len(my_accts) > 1:
            df['DUMMYQTY_AVG'] = df[my_accts].astype(float).sum(axis=1)
            df = df.sort_values(by=['DUMMYQTY_AVG'], ascending=[False])
        else:
            df = df.sort_values(by=['Total_Qty'], ascending=[False])
    elif sortby == "wt" and (1 in Price.pricing_accounts_sel):
        my_accts = [(b + '_Wt') for a, b in zip(Price.pricing_accounts_sel, Price.pricing_accounts) if a != 0]
        if len(my_accts) > 1:
            df['DUMMYWT_AVG'] = df[my_accts].astype(float).mean(axis=1)
            df = df.sort_values(by=['DUMMYWT_AVG'], ascending=[False])
        else:
            df = df.sort_values(by=[my_accts[0]], ascending=[False])
    elif sortby == "ctd" and (1 in Price.pricing_accounts_sel):
        my_accts = [(b + '_CTD') for a, b in zip(Price.pricing_accounts_sel, Price.pricing_accounts) if a != 0]
        if len(my_accts) > 1:
            df['DUMMYCTD_AVG'] = df[my_accts].astype(float).mean(axis=1)
            df = df.sort_values(by=['DUMMYCTD_AVG'], ascending=[False])
        else:
            df = df.sort_values(by=[my_accts[0]], ascending=[False])
    elif sortby == "ctr" and (1 in Price.pricing_accounts_sel):
        my_accts = [(b + '_R') for a, b in zip(Price.pricing_accounts_sel, Price.pricing_accounts) if a != 0]
        if len(my_accts) > 1:
            df['DUMMY_AVG'] = df[my_accts].astype(float).mean(axis=1)
            df = df.sort_values(by=['DUMMY_AVG'], ascending=[False])
        else:
            df = df.sort_values(by=[my_accts[0]], ascending=[False])
    elif sortby == 'rsk':
        def sort_rtg(x):
            if x == 'AAA+':
                return 1
            elif x == 'AAA':
                return 2
            elif x == 'AAA-':
                return 3
            elif x == 'AA+':
                return 4
            elif x == 'AA':
                return 5
            elif x == 'AA-':
                return 6
            elif x == 'A+':
                return 7
            elif x == 'A':
                return 8
            elif x == 'A-':
                return 9
            elif x == 'BBB+':
                return 10
            elif x == 'BBB':
                return 11
            elif x == 'BBB-':
                return 12
            elif x == 'BB+':
                return 13
            elif x == 'BB':
                return 14
            elif x == 'BB-':
                return 15
            elif x == 'B+':
                return 16
            elif x == 'B':
                return 17
            elif x == 'B-':
                return 18
            elif x == 'CCC+':
                return 19
            elif x == 'CCC':
                return 20
            elif x == 'CCC-':
                return 21
            elif x == 'CC+':
                return 22
            elif x == 'CC':
                return 23
            elif x == 'CC-':
                return 24
            elif x == 'C+':
                return 25
            elif x == 'C':
                return 26
            elif x == 'C-':
                return 27
            elif x == 'DDD+':
                return 28
            elif x == 'DDD':
                return 29
            elif x == 'DDD-':
                return 30
            elif x == 'DD+':
                return 31
            elif x == 'DD':
                return 32
            elif x == 'DD-':
                return 33
            elif x == 'D+':
                return 34
            elif x == 'D':
                return 35
            elif x == 'D-':
                return 36
            elif x == 'E':
                return 37
            elif x == 'F':
                return 38
            else:
                return 39
        df["SORT1"] = df["ASSET_CLASS"].apply(sort_asset)
        df["SORT2"] = df["ASSET_CLASS"].apply(sort_industry)
        df["SORT3"] = np.nan
        df["SORT3"] = df["RATING"].apply(sort_rtg)
        df.loc[df['ASSET_CLASS'] == 'SU', 'SORT3'] = df['RATING']
        df.loc[df['ASSET_CLASS'] == 'EQ', 'SORT3'] = df['TICKER']
        df.loc[df['ASSET_CLASS'] == 'OTHER', 'SORT3'] = df['TICKER']
        df.loc[df['CLASS'] == '30D', 'SORT3'] = 1
        df.loc[df['CLASS'] == 'YTC', 'SORT3'] = 2
        df.loc[df['CLASS'] == 'BULLET', 'SORT3'] = 3
        df.loc[df['CLASS'] == 'HQSD', 'SORT3'] = 4
        df.loc[df['CLASS'] == 'HQLD', 'SORT3'] = 5
        df.loc[df['CLASS'] == 'CR', 'SORT3'] = 6
        df.loc[df['CLASS'] == 'TR', 'SORT3'] = 7
        df["SORT4"] = np.nan
        df.loc[df['CLASS'] == 'GVCAD2', 'SORT4'] = 1
        df.loc[df['CLASS'] == 'GVCAD5', 'SORT4'] = 1
        df.loc[df['CLASS'] == 'GVCAD10', 'SORT4'] = 1
        df.loc[df['CLASS'] == 'GVCAD30', 'SORT4'] = 1
        df.loc[df['CLASS'] == 'GVUSD2', 'SORT4'] = 2
        df.loc[df['CLASS'] == 'GVUSD5', 'SORT4'] = 2
        df.loc[df['CLASS'] == 'GVUSD10', 'SORT4'] = 2
        df.loc[df['CLASS'] == 'GVUSD30', 'SORT4'] = 2
        df.loc[df['CLASS'] == 'GVOTHER', 'SORT4'] = 3
        df = df.sort_values(by=['SORT1', 'SORT2', 'SORT3', 'SORT4', 'YEARSW'], ascending=[True, True, True, True, True])
        df = df.drop(['SORT1', 'SORT2', 'SORT3', 'SORT4'], axis=1)
    elif sortby == 'iss':
        df["SORT1"] = df["ASSET_CLASS"].apply(sort_asset)
        df["SORT2"] = df["ASSET_CLASS"].apply(sort_industry)
        df["SORT3"] = np.nan
        df.loc[df['ASSET_CLASS'] == 'SU', 'SORT3'] = df['RATING']
        df.loc[df['ASSET_CLASS'] == 'EQ', 'SORT3'] = df['TICKER']
        df.loc[df['ASSET_CLASS'] == 'HY', 'SORT3'] = df['TICKER']
        df.loc[df['ASSET_CLASS'] == 'LN', 'SORT3'] = df['TICKER']
        df.loc[df['ASSET_CLASS'] == 'HG', 'SORT3'] = df['TICKER']
        df.loc[df['ASSET_CLASS'] == 'CV', 'SORT3'] = df['TICKER']
        df.loc[df['ASSET_CLASS'] == 'AB', 'SORT3'] = df['TICKER']
        df.loc[df['ASSET_CLASS'] == 'OTHER', 'SORT3'] = df['TICKER']
        df["SORT4"] = np.nan
        df.loc[df['CLASS'] == 'GVCAD2', 'SORT4'] = 1
        df.loc[df['CLASS'] == 'GVCAD5', 'SORT4'] = 1
        df.loc[df['CLASS'] == 'GVCAD10', 'SORT4'] = 1
        df.loc[df['CLASS'] == 'GVCAD30', 'SORT4'] = 1
        df.loc[df['CLASS'] == 'GVUSD2', 'SORT4'] = 2
        df.loc[df['CLASS'] == 'GVUSD5', 'SORT4'] = 2
        df.loc[df['CLASS'] == 'GVUSD10', 'SORT4'] = 2
        df.loc[df['CLASS'] == 'GVUSD30', 'SORT4'] = 2
        df.loc[df['CLASS'] == 'GVOTHER', 'SORT4'] = 3
        df = df.sort_values(by=['SORT1', 'SORT2', 'SORT3', 'SORT4', 'YEARSW'], ascending=[True, True, True, True, True])
        df = df.drop(['SORT1', 'SORT2', 'SORT3', 'SORT4'], axis=1)
    else:
        df["SORT1"] = df["ASSET_CLASS"].apply(sort_asset)
        df["SORT2"] = df["ASSET_CLASS"].apply(sort_industry)
        df["SORT3"] = np.nan
        df.loc[df['ASSET_CLASS'] == 'SU', 'SORT3'] = df['RATING']
        df.loc[df['ASSET_CLASS'] == 'EQ', 'SORT3'] = df['TICKER']
        df.loc[df['ASSET_CLASS'] == 'OTHER', 'SORT3'] = df['TICKER']
        df["SORT4"] = np.nan
        df.loc[df['ASSET_CLASS'] == 'SU', 'SORT4'] = df['TICKER']
        df.loc[df['CLASS'] == '30D', 'SORT4'] = 1
        df.loc[df['CLASS'] == 'YTC', 'SORT4'] = 2
        df.loc[df['CLASS'] == 'BULLET', 'SORT4'] = 3
        df.loc[df['CLASS'] == 'HQSD', 'SORT4'] = 4
        df.loc[df['CLASS'] == 'HQLD', 'SORT4'] = 5
        df.loc[df['CLASS'] == 'CR', 'SORT4'] = 6
        df.loc[df['CLASS'] == 'TR', 'SORT4'] = 7
        df.loc[df['CLASS'] == 'IGCAD2', 'SORT4'] = 1
        df.loc[df['CLASS'] == 'IGCAD5', 'SORT4'] = 2
        df.loc[df['CLASS'] == 'IGCAD10', 'SORT4'] = 3
        df.loc[df['CLASS'] == 'IGCAD30', 'SORT4'] = 4
        df.loc[df['CLASS'] == 'IGUSD2', 'SORT4'] = 5
        df.loc[df['CLASS'] == 'IGUSD5', 'SORT4'] = 6
        df.loc[df['CLASS'] == 'IGUSD10', 'SORT4'] = 7
        df.loc[df['CLASS'] == 'IGUSD30', 'SORT4'] = 8
        df.loc[df['CLASS'] == 'IGOTHER', 'SORT4'] = 9
        df.loc[df['CLASS'] == 'GVCAD2', 'SORT4'] = 1
        df.loc[df['CLASS'] == 'GVCAD5', 'SORT4'] = 2
        df.loc[df['CLASS'] == 'GVCAD10', 'SORT4'] = 3
        df.loc[df['CLASS'] == 'GVCAD30', 'SORT4'] = 4
        df.loc[df['CLASS'] == 'GVUSD2', 'SORT4'] = 5
        df.loc[df['CLASS'] == 'GVUSD5', 'SORT4'] = 6
        df.loc[df['CLASS'] == 'GVUSD10', 'SORT4'] = 7
        df.loc[df['CLASS'] == 'GVUSD30', 'SORT4'] = 8
        df.loc[df['CLASS'] == 'GVOTHER', 'SORT4'] = 9
        df = df.sort_values(by=['SORT1', 'SORT2', 'SORT3', 'SORT4', 'YEARSW'], ascending=[True, True, True, True, True])
        df = df.drop(['SORT1', 'SORT2', 'SORT3', 'SORT4'], axis=1)

    cols = df.columns
    for c in cols:
        if str(c)[-3:] == "1_x" or str(c)[-2:] == "1_y":
            df = df.drop([c], axis=1)

    df['SPREAD_FINAL_YEST'] = df['SPREAD_FINAL_YEST'].map(lambda x: stringer(x, 1))
    df['SPREAD_FINAL'] = df['SPREAD_FINAL'].map(lambda x: stringer(x, 1))
    df['SPREAD_CHG'] = df['SPREAD_CHG'].map(lambda x: stringer(x, 1))

    df['PRICE_FINAL_YEST'] = df['PRICE_FINAL_YEST'].map(lambda x: stringer(x, 3))
    df['PRICE_FINAL'] = df['PRICE_FINAL'].map(lambda x: stringer(x, 3))
    df['PRICE_BBG'] = df['PRICE_BBG'].map(lambda x: stringer(x, 3))
    df['PRICE_CHG'] = df['PRICE_CHG'].map(lambda x: stringer(x, 2))
    df['DOLLAR_CHG'] = df['DOLLAR_CHG'].map(lambda x: stringer(x, 3))
    df['Total_Qty'] = df['Total_Qty'].map(lambda x: stringer(x, 0))

    df['YEARSW'] = df['YEARSW'].map(lambda x: stringer(x, 1))
    df['YIELD_WORST'] = df['YIELD_WORST'].map(lambda x: stringer(x, 2))
    for acct in Price.pricing_accounts:
        df[acct + '_Wt'] = df[acct + '_Wt'].map(lambda x: stringer(x, 2))
        df[acct + '_Qty'] = df[acct + '_Qty'].map(lambda x: stringer(x, 0))
        df[acct + '_R'] = df[acct + '_R'].map(lambda x: stringer(x, 2))
        df[acct + '_CTD'] = df[acct + '_CTD'].map(lambda x: stringer(x, 2))

    df = df[(df['ASSET_CLASS'].isin(['SU', 'LN', 'HY', 'HG', 'GV', 'EQ', 'AB', 'CV', 'CP', 'OTHER']))]

    return df

def pricing_filter_df(df, ticker='', tag='', industry='NA', cyc='NA', rank='NA'):
    # filter tickers
    if str(ticker) != '':
        df['TICKER'] = df['TICKER'].astype(str)
        df = df[df['TICKER'].str.contains(str(ticker).upper(), na=False)]

    # filter tags
    if str(tag) != '':
        df['TAG'] = df['TAG'].astype(str)
        df['TAGU'] = df['TAG'].astype(str)
        df['TAGU'] = df['TAGU'].str.upper()
        df = df[df['TAGU'].str.contains(str(tag).upper(), na=False)]

    # filter industry
    if str(industry) not in ['', 'NA']:
        try:
            ind_list = Price.ind_lookup_dict[str(industry)]
            df = df[df['IND'].isin(ind_list)]
        except:
            pass

    # filter cyc
    if str(cyc) not in ['', 'NA']:
        try:
            df['CYC'] = df['CYC'].astype(str)
            df = df[df['CYC'].isin([cyc])]
        except:
            pass

    # filter rank
    if str(rank) not in ['', 'NA']:
        try:
            df['RANK'] = df['RANK'].astype(str)
            df = df[df['RANK'].isin([rank])]
        except:
            pass

    # if no account filter, no group filter
    if (1 not in Price.pricing_accounts_sel) and (1 not in Price.pricing_groups_sel):
        return df

    # if yes account filter, no group filter
    elif (1 in Price.pricing_accounts_sel) and (1 not in Price.pricing_groups_sel):
        my_accts = [(b + '_Wt') for a, b in zip(Price.pricing_accounts_sel, Price.pricing_accounts) if a != 0]
        df = df[~df[my_accts].isna().all(1)]
        return df

    # if no account filter, yes group filter
    elif (1 not in Price.pricing_accounts_sel) and (1 in Price.pricing_groups_sel):
        my_groups = [b for a, b in zip(Price.pricing_groups_sel, Price.pricing_groups) if a != 0]
        df = df.loc[df['CLASS'].isin(my_groups)]
        return df

    # if yes account filter, yes group filter
    else:
        my_accts = [(b + '_Wt') for a, b in zip(Price.pricing_accounts_sel, Price.pricing_accounts) if a != 0]
        my_groups = [b for a, b in zip(Price.pricing_groups_sel, Price.pricing_groups) if a != 0]
        df = df[~df[my_accts].isna().all(1)]
        df = df.loc[df['CLASS'].isin(my_groups)]
        return df


def pricing_format_df(df):
    df['SPREAD_FINAL_YEST'] = df['SPREAD_FINAL_YEST'].astype('float')
    df['SPREAD_FINAL_YEST'] = df['SPREAD_FINAL_YEST'] * 2
    df['SPREAD_FINAL_YEST'] = df['SPREAD_FINAL_YEST'].round(0) / 2
    df['SPREAD_FINAL_YEST'] = df['SPREAD_FINAL_YEST'].round(1)

    df['SPREAD_BBG'] = df['SPREAD_BBG'].astype('float')
    df['SPREAD_BBG'] = df['SPREAD_BBG'] * 2
    df['SPREAD_BBG'] = df['SPREAD_BBG'].round(0) / 2
    df['SPREAD_BBG'] = df['SPREAD_BBG'].round(1)

    df['SPREAD_FINAL'] = df['SPREAD_FINAL'].astype('float')
    df['SPREAD_FINAL'] = df['SPREAD_FINAL'] * 2
    df['SPREAD_FINAL'] = df['SPREAD_FINAL'].round(0) / 2
    df['SPREAD_FINAL'] = df['SPREAD_FINAL'].round(1)

    df['PRICE_FINAL_YEST'] = df['PRICE_FINAL_YEST'].astype('float')
    df['PRICE_FINAL_YEST'] = df['PRICE_FINAL_YEST'].round(3)
    df['PRICE_BBG'] = df['PRICE_BBG'].astype('float')
    df['PRICE_BBG'] = df['PRICE_BBG'].round(3)
    df['PRICE_FINAL'] = df['PRICE_FINAL'].astype('float')
    df['PRICE_FINAL'] = df['PRICE_FINAL'].round(3)

    df['SPREAD_CHG'] = df['SPREAD_FINAL'] - df['SPREAD_FINAL_YEST']
    df['SPREAD_CHG'] = df['SPREAD_CHG'].round(1)

    df['DOLLAR_CHG'] = (df['PRICE_FINAL'] - df['PRICE_FINAL_YEST'])
    df['DOLLAR_CHG'] = df['DOLLAR_CHG'].round(3)
    df['PRICE_CHG'] = (df['DOLLAR_CHG']) / df['PRICE_FINAL_YEST'] * 100.0
    df['PRICE_CHG'] = df['PRICE_CHG'].round(2)

    df['ANY_OV'] = np.nan
    df['ANY_SPREAD_OV'] = np.nan
    df = df[~df.index.duplicated(keep='first')]
    try:
        df.loc[df['SPREAD_OV'].notnull(), 'ANY_OV'] = "y"
    except:
        pass
    try:
        df.loc[df['BENCH_OV'].notnull(), 'ANY_OV'] = "y"
    except:
        pass
    try:
        df.loc[df['PRICE_OV'].notnull(), 'ANY_OV'] = "y"
    except:
        pass
    # df.loc[df['SPREAD_OV'].notnull() | df['PRICE_OV'].notnull() | df['BENCH_OV'].notnull(), 'ANY_OV'] = "y"
    # print(df[['SPREAD_OV', 'PRICE_OV', 'BENCH_OV']].head(3))
    # df.loc[df['SPREAD_OV'].notnull(), 'ANY_SPREAD_OV'] = "y"

    for account in Price.pricing_accounts:
        df[account + '_Wt'] = df[account + '_Wt'].astype('float')
        df[account + '_Wt'] = df[account + '_Wt'].astype('float')
        df[account + '_Wt'] = df[account + '_Wt'].round(2)
        df[account + '_R'] = df[account + '_Wt'] * df['PRICE_CHG']
        df[account + '_R'] = df[account + '_R'].astype('float')
        df[account + '_R'] = df[account + '_R'].round(2)
    return df

def pricing_clean_classification(x):
    if x in ['IG_EUR_2Y', 'IG_EUR_5Y', 'IG_EUR_10Y', 'IG_EUR_30Y', 'IG_GBP_2Y', 'IG_GBP_5Y', 'IG_GBP_10Y',
               'IG_GBP_30Y', 'IG_AUD_2Y', 'IG_AUD_5Y', 'IG_AUD_10Y', 'IG_AUD_30Y']:
        return 'IGOTHER'
    elif x in ['GOV_EUR_2Y', 'GOV_EUR_5Y', 'GOV_EUR_10Y', 'GOV_EUR_30Y', 'GOV_GBP_2Y', 'GOV_GBP_5Y', 'GOV_GBP_10Y',
               'GOV_GBP_30Y', 'GOV_AUD_2Y', 'GOV_AUD_5Y', 'GOV_AUD_10Y', 'GOV_AUD_30Y']:
        return 'GVOTHER'
    elif x == '30D_ROLL':
        return '30D'
    elif x == 'SDHY':
        return 'HQSD'
    elif x == 'LDHY':
        return 'HQLD'
    elif x == 'YTC_STEPUP':
        return 'YTC'
    elif x == 'IG_CAD_2Y':
        return 'IGCAD2'
    elif x == 'IG_CAD_5Y':
        return 'IGCAD5'
    elif x == 'IG_CAD_10Y':
        return 'IGCAD10'
    elif x == 'IG_CAD_30Y':
        return 'IGCAD30'
    elif x == 'IG_USD_2Y':
        return 'IGUSD2'
    elif x == 'IG_USD_5Y':
        return 'IGUSD5'
    elif x == 'IG_USD_10Y':
        return 'IGUSD10'
    elif x == 'IG_USD_30Y':
        return 'IGUSD30'
    elif x == 'GOV_CAD_2Y':
        return 'GVCAD2'
    elif x == 'GOV_CAD_5Y':
        return 'GVCAD5'
    elif x == 'GOV_CAD_10Y':
        return 'GVCAD10'
    elif x == 'GOV_CAD_30Y':
        return 'GVCAD30'
    elif x == 'GOV_USD_2Y':
        return 'GVUSD2'
    elif x == 'GOV_USD_5Y':
        return 'GVUSD5'
    elif x == 'GOV_USD_10Y':
        return 'GVUSD10'
    elif x == 'GOV_USD_30Y':
        return 'GVUSD30'
    else:
        return x
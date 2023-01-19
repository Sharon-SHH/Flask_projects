import getpass
import os
import datetime
from Flask_projects_learning.flask_sqlAlchemy.account import Account
from pyodbc import connect
class Common:
    filepath = os.path.realpath(__file__)
    parent = os.path.dirname(filepath)
    monthly_name = 'monthly_returns.txt'
    yearly_name = 'yearly_returns.txt'
    username_names = {'MSAMIM': 'Masood', 'HSHI': 'Sharon', 'ATUER': 'Mr Grinch', 'APRENC': 'Adrian', 'PSANDHU': 'Paul',
                      'YWOO': 'Younsu', 'BDAROZA': 'Braxtin', 'GMCKAY': 'Gord'}
    return_row_names = ['Total', 'Term Loans', 'High Yield', 'HY_30 Day', 'HY_Yld to Call', 'HY_Bullets', 'HY_HQ SD', 'HY_HQ LD',
            'HY_Credit Risk', 'HY_Tot Return', 'Inv Grade', 'IG_CAD', 'IG_CAD_2y', 'IG_CAD_5y', 'IG_CAD_10y',
            'IG_CAD_30y', 'IG_USD', 'IG_USD_2y', 'IG_USD_5y', 'IG_USD_10y', 'IG_USD_30y', 'IG_Other', 'Govs',
            'GV_CAD', 'GV_CAD_2y', 'GV_CAD_5y', 'GV_CAD_10y', 'GV_CAD_30y', 'GV_USD', 'GV_USD_2y', 'GV_USD_5y',
            'GV_USD_10y', 'GV_USD_30y', 'GV_Other', 'CDX', 'Equities', 'Other']
    return_column_names = ['ETFs']  # create columns for the returns
    for acc in Account.query:
        return_column_names.append(str(acc.account_id))
    HY_members = ['30 Day', 'Yld to Call', 'Bullets', 'HQ SD', 'HQ LD', 'Credit Risk', 'Tot Return', 'CAD', 'USD',
                     'Other']
    prior_show = ['Total', 'Term Loans', 'High Yield', 'Inv Grade', 'Govs', 'CDX', 'Equities', 'Other']
    date_tday = (datetime.date.today()).strftime("%Y-%m-%d")
    cnxn = connect('DRIVER={SQL Server};SERVER=HDSQL01\INS2;DATABASE=MARRET;MARS_Connection=Yes;')
    etf_returns = {}
    etf_returns_list = ['HYG US EQUITY', 'BKLN US EQUITY', 'LQD US EQUITY', 'IEF US EQUITY', 'XGB CN EQUITY',
                        'XCB CN EQUITY', 'SPY US EQUITY', 'XSB CN EQUITY']
    asset = ['SU', 'LN', 'HY', 'HG', 'GV', 'AB', 'CV', 'EQ', 'BT', 'Other']
    rates = ['AAA+', 'AAA', 'AAA-', 'AA+', 'AA', 'AA-', 'A+', 'A', 'A-', 'BBB+', 'BBB', 'BBB-', 'BB+', 'BB', 'BB-',
             'B+', 'B', 'B-', 'CCC+', 'CCC', 'CCC-', 'CC+', 'CC', 'CC-', 'C+', 'C', 'C-', 'DDD+', 'DDD', 'DDD-',
             'DD+', 'DD', 'DD-', 'D+', 'D', 'D-', 'E', 'F', 'Other']


    def __init__(self):
        try:
            self.username = str(getpass.getuser()).upper()
            print('Hello ' + self.username_names[self.username])
            if self.username in ['BDAROZA', 'ATUER', 'APRENC', 'GMCKAY']:
                self.test_page = 'n'
            elif self.username == 'HSHI':
                self.test_page = 'pricing'  # sharon only needs pricing tab
            elif self.username == 'MSAMIM' or 'PSANDHU':
                self.test_page = 'easymode'
        except Exception:
            #username = 'BDAROZA'
            username = str(os.path.split(os.path.expanduser('~'))[-1]).upper()
            print('Hello ' + username)
            return


class Price():
    pricing_push_wait, pricing_push_confirm = 'n', 'n'
    pricing_last_update = 'Updated: N/A'
    pricing_users = {'Sharon': 'HSHI', 'Adam': 'ATUER', 'Adrian': 'APRENC', 'Paul': 'PSANDHU', 'Braxtin': 'BDAROZA',
                     'Younsu': 'YWOO', 'Gord': 'GMCKAY', 'Masood': 'MSAMIM', }

    pricing_mandates = ['Government', 'Credit', 'Hedged', 'Short Duration', 'High Yield']

    pricing_accounts = ['181', '183', '190', '125', '175', '184', '131', '218', '192', '174', '179', '106', '162']
    pricing_accounts_mandates = {'181': 'Government',
                                 '183': 'Government',
                                 '190': 'Government',
                                 '125': 'Credit',
                                 '175': 'Credit',
                                 '184': 'Credit',
                                 '131': 'Hedged',
                                 '218': 'Hedged',
                                 '192': 'Short Duration',
                                 '174': 'Short Duration',
                                 '179': 'Short Duration',
                                 '106': 'High Yield',
                                 '162': 'High Yield'}
    pricing_groups = ['30D', 'YTC', 'BULLET', 'HQSD', 'HQLD', 'CR', 'TR', 'LN', 'IGCAD2', 'IGCAD5', 'IGCAD10',
                      'IGCAD30', 'IGUSD2', 'IGUSD5', 'IGUSD10', 'IGUSD30', 'IGOTHER', 'GVCAD2', 'GVCAD5', 'GVCAD10',
                      'GVCAD30', 'GVUSD2', 'GVUSD5', 'GVUSD10', 'GVUSD30', 'GVOTHER', 'SU', 'EQ', 'OTHER']

    ind_lookup_dict = {'Autos': ['AUTOS'],
                       'Banks': ['BANKING'],
                       'Capital Goods': ['CAPITAL GOODS'],
                       'Consumer Discr': ['CONS DISCR', 'LEISURE', 'RETAIL'],
                       'Consumer Staples': ['CONS STAPLES'],
                       'Energy': ['ENERGY'],
                       'Financials': ['FINANCIALS', 'INSURANCE'],
                       'Healthcare': ['HEALTHCARE'],
                       'Materials': ['MATERIALS'],
                       'Media': ['MEDIA'],
                       'Metals & Mining': ['METALS/MINING'],
                       'Other': ['OTHER'],
                       'Real Estate': ['REAL ESTATE'],
                       'Services': ['SERVICES'],
                       'Technology': ['TECHNOLOGY'],
                       'Telecom': ['TELECOM'],
                       'Transport': ['TRANSPORTATION'],
                       'Utilities': ['UTILITIES']}
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

    pricing_groups_sel = [0] * len(pricing_groups)
    pricing_accounts_sel = [0] * len(pricing_accounts)

    wait_table, refresh_count = 0, 0
    pricing_ov_ig_user, pricing_ov_hy_user = 'ATUER', 'APRENC'  # if master_settings.username != 'GMCKAY' else 'GMCKAY'
    pricing_attribs_stats = {}
    pricing_movers_styles = []
    pricing_table_styles = []
    tool_select_value = 'STATS'
    movers_value_asset = 'All'
    movers_value_sort = 'pd'
    attrib_chart_options = [{'label': account, 'value': account} for account in pricing_accounts]
    attrib_index_list = ['UST Index', 'UST 30 Index', 'USD IG Index', 'USD HY Index', 'CAD IG Index', 'USD BB Index',
                         'USD B Index', 'USD CCC Index', 'SPX Index', 'Dow Index', 'EM Index']
    movers_date1w = ''
    movers_date1m = ''
    bets_refresh_count = 0

    pricing_buttons_mandates = []
    pricing_buttons_accounts = []
    pricing_buttons_mandates_stats = []
    pricing_buttons_accounts_stats = []


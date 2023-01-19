from app import db, Returns
import datatable_holder
from common import Common

# create an instance for the Return object
# TODO: db.session.add() one element
# TODO: db.session.add_all([element1, element2])


def update_returns():
    df_res = datatable_holder.holder(Common.cnxn)
    for a in df_res.itertuples():
        returns = Returns(cusip_code=a.Index, security_name=a.SECURITY_NAME, asset_class=a.ASSET_CLASS, class_=a.CLASS,
                          yield_worst=a.YIELD_WORST, price_final_yest=a.PRICE_FINAL_YEST, price_final=a.PRICE_FINAL,
                          dollar_chg=a.DOLLAR_CHG, price_chg=a.PRICE_CHG,
                          spread_final_yest=a.SPREAD_FINAL_YEST, spread_final=a.SPREAD_FINAL, spread_chg=a.SPREAD_CHG,
                          benchname_final=a.BENCHNAME_FINAL,
                          source_ov=a.SOURCE_OV, price_ov_yest=a.PRICE_OV_YEST, price_ov=a.PRICE_OV,
                          spread_ov=a.SPREAD_OV, bench_ov=a.BENCH_OV, new_i=a.NEWI_OV
                          )
        db.session.add(returns)
    db.session.commit()  # submit the changes to the table


import os
from flask import Flask, render_template, request, jsonify
from app import app, db
from setup_sqlAlchemy import update_returns, Returns

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/data')
def data():
    update_returns()
    query = Returns.query
    # search filter
    search = request.args.get('search[value]')
    if search:
        query = query.filter(db.or_(
            Returns.cusip.like(f'%{search}%'),
            Returns.security_name.like(f'%{search}%')
        ))
    total_filtered = query.count()

    # sorting
    order = []
    i = 0
    while True:
        col_index = request.args.get(f'order[{i}][column]')
        if col_index is None:
            break
        col_name = request.args.get(f'columns[{col_index}][data]')
        if col_name not in ['cusip_code', 'security_name']:
            col_name = 'cusip_code'
        descending = request.args.get(f'order[{i}][dir]') == 'desc'
        col = getattr(Returns, col_name)
        if descending:
            col = col.desc()
        order.append(col)
        i += 1

    # pagination
    start = request.args.get('start', type=int)
    length = request.args.get('length', type=int)
    query = query.offset(start).limit(length)

    # response
    return {
        'data': [returns.to_dict() for returns in query],
        'recordsFiltered': total_filtered,
        'recordsTotal': Returns.query.count(),
        'draw': request.args.get('draw', type=int),
    }

if __name__ == '__main__':
   app.run(port=9001)
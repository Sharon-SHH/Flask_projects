from flask import Flask, render_template, request

app = Flask(__name__)

##########################################
# create a html, Sign up. Successfully, turn to another page. Share the same header and foot.
##########################################
@app.route('/thankyou', methods=['GET', 'POST'])
def thankyou():
    #first_name = request.form(['first'])
    first_name = request.args.get('first')
    last_name = request.args.get('last')
    return render_template('thankyou.html', firstName=first_name, lastName=last_name)

@app.route('/signup')
def signup():
    return render_template('signup.html')

@app.route('/')
def index():
    return render_template('index.html')

if __name__ == '__main__':
    app.run(debug=True)
from flask import Flask
from flask import Flask, render_template
import pandas as pd
from datetime import date

data = pd.read_csv('output.csv', index_col=False)
pred_date = '2021-01-31'
data = data.iloc[:,1:]
# print(data)

output_table = data.values.tolist()
# print(list_of_data)

app = Flask(__name__)


@app.route("/")
def prediction_results():
    return render_template('main.html', Date=date.today().strftime("%Y-%m-%d"), Pred_date=pred_date,
                           result=output_table)


app.run(host='127.0.0.1', port=5000, debug=True)


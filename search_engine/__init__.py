from general_spitishop import *

from flask import Flask, render_template, request
import pandas as pd

app = Flask(__name__)

@app.server.route('/spitishop_home', methods=['GET'])
def spitishop_home():
    """
        Opens the spitishop SE home page
    """
    
    return render_template('home_spitishop1.html')

@app.server.route('/spitishop_result', methods=['POST', 'GET'])
def spitishop_result():
    if request.method == 'POST':
        query = request.form.to_dict()
        #print(query)

        results, search_flag = get_results_spitishop(query['search'], query['category'])
        
        #return str(results)

        return render_template("result_spitishop1.html", result1=query, result2=results, search_flag=search_flag)

if __name__ == '__main__':
    app.run(debug=True)

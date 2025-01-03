from flask import Flask, render_template, request
import sqlite3
import os

# Flask app initialization
app = Flask(__name__)

# Database path
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATABASE_NAME = os.path.join(SCRIPT_DIR, "vsd_read.db")


def fetch_data_from_table(table_name):
    """
    Fetch the last 10 rows from the specified SQLite database table.
    """
    conn = sqlite3.connect(DATABASE_NAME)
    cursor = conn.cursor()

    # Fetch the last 10 rows
    query = f"SELECT * FROM {table_name} ORDER BY id DESC LIMIT 10"
    cursor.execute(query)
    rows = cursor.fetchall()

    # Fetch the column names
    column_names = [description[0] for description in cursor.description]

    conn.close()
    return rows, column_names


@app.route("/", methods=["GET", "POST"])
def index():
    """
    Route to display data from a selected table using a dropdown list.
    """
    tables = ["vsd_temp", "vsd_current"]  # List of available tables
    selected_table = "vsd_temp"  # Default table to display

    if request.method == "POST":
        selected_table = request.form.get("table_name", "vsd_temp")

    try:
        # Fetch data from the selected table
        data, columns = fetch_data_from_table(selected_table)

        # Render the template with data
        return render_template(
            "table_dropdown.html", tables=tables, selected_table=selected_table, rows=data, columns=columns
        )

    except Exception as e:
        return f"Error: {e}"


if __name__ == "__main__":
    app.run(debug=True)

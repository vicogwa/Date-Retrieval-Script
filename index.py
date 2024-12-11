import csv
import mysql.connector
from datetime import datetime

def fetch_user_id_from_unique_code(connection, unique_code):
    """
    Fetch user ID from vendoritems table using unique_code.
    """
    query = """
        SELECT user FROM irecharg_irchrg.vendoritems
        WHERE title = %s;
    """
    cursor = connection.cursor(dictionary=True)
    cursor.execute(query, (unique_code,))
    result = cursor.fetchone()
    cursor.close()
    return result['user'] if result else None


def fetch_balance_and_transaction_date(connection, user_id):
    """
    Fetch the balance_snapshot and datecreated from wallet_transactionitems table.
    If no result, check walletitems table.
    """
    # Query for wallet_transactionitems table
    query_wallet_transactionitems = """
        SELECT balance_snapshot, datecreated
        FROM irecharg_irchrg.wallet_transactionitems
        WHERE user = %s AND datecreated <= '2024-10-31'
        ORDER BY datecreated DESC
        LIMIT 1;
    """
    
    # Query for walletitems table (fallback)
    query_walletitems = """
        SELECT balance, datecreated
        FROM irecharg_irchrg.walletitems
        WHERE user = %s
        LIMIT 1;
    """

    cursor = connection.cursor(dictionary=True)
    
    # First attempt: fetch from wallet_transactionitems
    cursor.execute(query_wallet_transactionitems, (user_id,))
    result = cursor.fetchone()

    # If a result is found in wallet_transactionitems, return it
    if result:
        cursor.close()
        return result['balance_snapshot'], result['datecreated']
    
    # If no result from wallet_transactionitems, fallback to walletitems table
    cursor.execute(query_walletitems, (user_id,))
    result = cursor.fetchone()
    cursor.close()

    # Return the result from walletitems if found, otherwise return None
    if result:
        return result['balance'], result['datecreated']
    
    return None, None  # Return None if no data is found in either table


def standardize_date(date_str):
    """
    Converts a date string to the format '%Y-%m-%d'.
    Handles different input formats like '%d/%m/%Y', '%m/%d/%Y', and '%Y-%m-%d'.
    """
    if not date_str:
        return None
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y", "%Y%m%d", "%d-%m-%Y", "%m-%d-%Y", "%Y/%m/%d"):
        try:
            return datetime.strptime(date_str, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    print(f"Warning: Unable to parse date '{date_str}'. Leaving it unchanged.")
    return date_str  # Return as-is if parsing fails


def process_file(file_path, output_path, db_config):
    """
    Processes the input file, fills missing columns using database values, and writes the required columns to the output file.
    """
    connection = mysql.connector.connect(**db_config)

    try:
        with open(file_path, mode='r') as infile, open(output_path, mode='w', newline='') as outfile:
            reader = csv.DictReader(infile)
            fieldnames = ['unique_code', 'last_balance_snapshot', 'last_transaction_date', 'user_id']
            writer = csv.DictWriter(outfile, fieldnames=fieldnames)
            writer.writeheader()

            for row in reader:
                # Initialize columns
                result_row = {'unique_code': row.get('unique_code')}

                # Fetch user_id if missing from unique_code
                user_id = row.get('user_id')
                if not user_id and row.get('unique_code'):
                    user_id = fetch_user_id_from_unique_code(connection, row['unique_code'])
                    result_row['user_id'] = user_id

                # Fetch last_balance_snapshot and datecreated
                if user_id:
                    last_balance_snapshot, datecreated = fetch_balance_and_transaction_date(connection, user_id)
                    result_row['last_balance_snapshot'] = last_balance_snapshot
                    result_row['last_transaction_date'] = standardize_date(datecreated)

                # Write the result row (only the necessary columns)
                writer.writerow(result_row)

        print(f"Processing complete. Updated file saved to '{output_path}'.")

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        connection.close()

# --- Execution ---
if __name__ == "__main__":
    # Database configuration
    db_config = {
        'host': '167.99.171.73',
        'user': 'irecharg_emmanuel',
        'password': '12#Emmanuel@$',
        'database': 'irecharg_irchrg'
    }

    # Input and output file paths
    input_file = 'Balance-Request updated.csv'
    output_file = 'Balance-Request updated_output.csv'

    # Process the file
    process_file(input_file, output_file, db_config)

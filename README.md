# Scripts_for_sales_control_in_my_brewery
Scripts made in order to automate the records and a quick analysis of my brewery's sales. The inputs are handled only by me, thus the scripts are tools for my own purpose.

1. insert_venta.py takes the sale's information as user's inputs and then injects it into the respective database. Then the database is updated with the new sale.

2. update_db.py modifies the current database entries in which the payment date is none by placing the final payment date or an updated payment date and amount. This is, when the client gets out of his debt or makes a partial payment. 

Further projects:

3. summ_stat.py as a summary statistics generator which takes time periods (yearly, monthly, daily) as user's input, and return useful insights about sales in that period.

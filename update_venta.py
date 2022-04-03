#Librerias
import sqlite3 as sql
import pandas as pd
import numpy as np

def main():
    """Read the active debts in sales data base and update them.
    1. Read the database and extract the active debts
    2. Take users's input and create a dataframe with them
    3. Show in console the new entry and ask for confirmation
    4. Make the injection to the respective table    
    """
    # Open and connect to database
    connection =sql.connect('Ventas.db')
    cursor=connection.cursor()

    ################################### Setting up the framework
    #Query 
    query = 'SELECT * FROM Deudores;'
    cursor=connection.cursor()
    cursor.execute(query)
    # Table and rows respectively 
    raw_table = cursor.fetchall()
    cols_list = [i[0] for i in cursor.description]
    # Creating the DataFrame to work with
    deudores=pd.DataFrame(raw_table)
    deudores.columns=cols_list
    deudores['Fecha_adelanto'] = pd.to_datetime(deudores['Fecha_adelanto'], format='%d-%m-%Y')


    ################################# Take and show current data
    # Last dates debts
    idx_ultimas_fechas = deudores.groupby("id")["Fecha_adelanto"].idxmax()
    entries_ultimas_fechas = deudores.loc[idx_ultimas_fechas]
    # Take just active debts
    deudas_existentes = entries_ultimas_fechas[entries_ultimas_fechas['Monto_rest']!=0].drop('Monto_adelanto', axis=1).set_index('id')
    # Show for user's sake
    print("Lista de deudas: \n {}".format(deudas_existentes))


    ################################# Updating database information
    # id input; take the id of the debt user wants to update
    id_col = input('Por favor escriba el id de la deuda que desea actualizar, e.g. "Rup4" \n')
    assert(id_col in deudas_existentes.index), "Por favor inserta un id de venta válido."
    cliente_col = deudas_existentes.loc[id_col,'Cliente']

    # date input
    date_col = input('Por favor escriba la fecha en la que se pagó un adelanto (dd-mm-aaaa), e.g. 30-03-2022 \n')
    try: 
        fecha = pd.to_datetime(date_col, format='%d-%m-%Y')
    except:
        raise ValueError('No introdujo una fecha valida o no está en el formato requerido dia-mes-año.')
    # Important to assert time consistency since the first script's step is to read the last dates entries
    assert(fecha > deudas_existentes.loc[id_col, 'Fecha_adelanto']), "La fecha introducida debe ser más actual que la última fecha de adelanto."

    # income input
    monto_ad_col = float(input("Por favor indique cuánto se dio de adelanto. \n"))


    ################################ Show the preliminar view of the new entry
    # Calculate how much it lefts to pay 
    monto_res_col = deudas_existentes.loc[id_col, 'Monto_rest'] - monto_ad_col
    assert(monto_res_col >= 0), 'El monto ingresado no puede ser mayor que la deuda previa.'
    # Generate the data frame containing the update info
    new_entry = [id_col, cliente_col, date_col, monto_ad_col, monto_res_col]
    update_df = pd.DataFrame([new_entry], columns=cols_list)

    print('Vista preliminar de la nueva entrada \n-----------------------------------------------------------------\
    -----------------------------')
    print(update_df, '\n')
    print('-----------------------------------------------------------------\
    -----------------------------')

    ################################ Confirmation for updating
    # Confirmation Input
    confirm = input('\n ¿Está seguro que desea realizar la actualización? si/no \n')

    # Updating the database
    if confirm == 'si':
        print('Corriendo ingesta')
    #¡¡¡¡¡¡¡NUNCA PONER REPLACE!!!!
        update_df.to_sql('Deudores', connection, if_exists='append', index=False)
        print('Ingesta finalizada')
    elif confirm == 'no':
        print('Cancelando...')
    else:
        raise ValueError('Opción no válida.')


        
    ############################## Closing and saving changes

    # Save changes and close database 
    cursor.close()
    connection.commit()
    connection.close()



    test_query='SELECT * FROM Ventas;'    
    try:
        cursor.execute(test_query)
        print('¡El cursor no se ha cerrado!. Ultima query: {}'.format(test_query))
    except:
        print('El cursor se cerró correctamente')
        
        
    try:
        connection.execute(test_query)
        print('¡La conexión no se ha cerrado!. Ultima query: {}'.format(test_query))
    except:
        print('La conexión se cerró correctamente')




if __name__ == '__main__':
    main()
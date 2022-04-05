import sqlite3 as sql
import pandas as pd
from IPython.display import display

def main():
    """ Modify the sales control database by inserting a new entry in both tables "Ventas" and "Lotes_vta".
    The former only accepts one row, which means a new sale entry with its respective columns. The later accepts
    as many rows as there is batches involved, this because a single sale entry is equivalent to an indefinite
    number of batches sold.
    Order of executions:
    1. Connect to database
    2. Extract last id from the database
    3. Fill client data
    4. Fill dates 
    5. Fill sale details
    6. Visualize an overview of the resulting entries
    7. Ask for confirmation and execute the injection
    8. Close the database, commit the result and test if it has been closed correctly 
    """
    
    # Open and connect to database
    connection =sql.connect('Ventas.db')
    cursor=connection.cursor()

    ############################## Generates data
    
    #Extract id and Cliente columns and convert the table to a dataframe
    query_client = 'SELECT id, Cliente FROM Ventas;'
    cursor.execute(query_client)
    raw_table = cursor.fetchall()
    
    #Take columns and insert them in the dataframe
    cols_list = [i[0] for i in cursor.description]
    client_info_df = pd.DataFrame(raw_table)
    client_info_df.columns=cols_list

    
    # id, Cliente 
    id_col, cliente_col = filling_cliente(client_info_df)

    # Fecha_vta, Fecha_cobro
    fecha_vta_col, fecha_cobro_col = filling_dates()

    # Encargado, No_cerv, Monto, Precio_u, Tipo_vta, Tipo_pago, Lotes (dict)
    encargado_col, no_cerv_col, monto_col, precio_u_col, tipo_vta_col, tipo_pago_col, lotes = filling_sale()



    ############################ Visualize new entry of table "Ventas"
    # Nueva fila
    new_row_vta = [id_col, cliente_col, fecha_vta_col, fecha_cobro_col, encargado_col, no_cerv_col, monto_col, precio_u_col, tipo_vta_col, tipo_pago_col]
    # Construcción del dataframe
    data_ventas = confirmation(new_row_vta, 'Ventas' ,cursor)

    ############################ Visualize new entry of table "Lotes_vta"
    data_lotes_vta = pd.DataFrame()
    for key, val in lotes.items():
        # Nueva fila (actual)
        new_entry_lotes = [id_col, key, val]
        # Construcción del dataframe (actual)
        new_row = confirmation(new_entry_lotes, 'Lotes_vta', cursor)
        # Construcción del dataframe (final)
        data_lotes_vta = pd.concat([data_lotes_vta, new_row], ignore_index=True, axis=0)

    ########################### Visualize new entry of table "Deudores"
    if fecha_cobro_col is None:
        monto_adelanto = float(input('¿Cuanto dio de adelanto el cliente?. Escriba 0 si no aplica. \n'))
        monto_rest = monto_col - monto_adelanto
        new_row_deuda = [id_col, cliente_col, fecha_vta_col, monto_adelanto, monto_rest]
        data_deuda = confirmation(new_row_deuda, 'Deudores' , cursor)


    ############################ Preliminar confirmation
    print('Vista preliminar de la nueva entrada \n-----------------------------------------------------------------\
-----------------------------')
    print('Ventas: \n',data_ventas, '\n')
    print('Lotes_vta: \n',data_lotes_vta, '\n')
    if fecha_cobro_col is None:
        print('Deudores: \n',data_deuda, '\n')
    print('-----------------------------------------------------------------\
-----------------------------')
    confirm = input('\n ¿Está seguro que desea realizar la ingesta? si/no \n')
    

    ############################ Injection
    if confirm == 'si':
        print('Corriendo ingesta')
        #¡¡¡¡¡¡¡NUNCA PONER REPLACE!!!!
        data_ventas.to_sql('Ventas', connection, if_exists='append', index=False)
        data_lotes_vta.to_sql('Lotes_vta', connection, if_exists='append', index=False)
        print('Ingesta finalizada')
        if fecha_cobro_col is None:
            data_deuda.to_sql('Deudores', connection, if_exists='append', index=False)
        
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



############################################### (id, client)-filling block 
def filling_cliente(database_df):
    """" Create the values of the client by reading the previous records in the database's
    dataframe.

    Args:
    database_df (DataFrame): Dataframe obtained from the database 

    Returns:
    id_col (str): Transaction id
    cliente_col (str): Client's name
    """
    def clave_generator(i_string):
        """ Reads a string and creates a duple with its string 
        and numeric parts separated.
        
        Args:
        i_string (string): Initial string to split

        Returns:
        i_string.strip(f_str) (str) : Final string part
        int(f_str) (int)            : Final numeric part
        """
    #List of numbers in the string
        lista=[i for i in i_string if i.isnumeric()]
    #Generate a string with the numbers
        f_str=''
        for num in lista:
            f_str = f_str + num
    #Return string and numeric parts
        return i_string.strip(f_str), int(f_str)

    #Read existent clients and ask for new's existence
    clientes_existentes = database_df['Cliente'].unique()
    print("Lista de clientes: \n {}".format(clientes_existentes))
    client_status = input('¿El cliente está en la lista? si/no \n')

    assert((client_status == 'si' or client_status == 'no')), 'Error, escribe "si" o "no".'

    #Deduce whether generating only id or a new pair id, client 
    if client_status == 'si':
        
        ############## Cliente column
        cliente_col = input('Escribe su nombre tal cual aparece en la lista sin las comillas \n')
        assert(cliente_col in clientes_existentes), 'Por favor escribe el nombre de un cliente de la lista.'
        
        #Take last matching id 
        database_df['id_num'] = database_df['id'].agg(lambda x: clave_generator(x)[1])
        last_id = database_df[database_df['Cliente']==cliente_col].sort_values('id_num')['id'].iloc[-1]
        
        #Separate numeric and string part of last_id
        clave, registro = clave_generator(last_id)  
        
        ############## id column
        id_col = clave + str(int(registro)+1)   
    elif client_status == 'no':
        ############## Cliente column
        cliente_col = input('Escribe el nombre del cliente, no podrá cambiarse despues \n')
        assert(cliente_col not in clientes_existentes), 'El nuevo cliente no puede estar en la lista de clientes existentes.'
        clave = input('Elige la clave con la cuál se llevará el id, e.g. Rupestre -> Rup \n')
        
        ############## id column
        id_col = clave + '1'
    
    return id_col, cliente_col

############################################### dates-filling block 

def filling_dates():   
    """ Require to the user to give the date in which the sale was given and when the payment was done.
    In the case the later is None, it would be modified in another script once the payment was received.

    Args: 
    None

    Returns
    fecha_vta_col (date): Sale date
    fecha_cobro_col (date or None): Payment date
    """ 
    def date_test(date):
        """ Ensure the date has the required structure

        Args:
        date (date): Date to be tested

        Returns:
        Pass or ValueError: Depending the test's success or failure
        """
        try: 
            fecha = pd.to_datetime(date, format='%d-%m-%Y')
        except:
            raise ValueError('No introdujo una fecha valida o no está en el formato requerido')

    ################# Sale date column
    fecha_vta_col = input('¿Cuándo se realizó la venta? (dd-mm-aaaa) \n')
    date_test(fecha_vta_col)


    ################# Payment date column
    fecha_cobro_col = input('Fecha de cobro. \
                            \nEscribe "no" si es que no se ha cobrado \
                            \n"same" si se cobró el mismo día que se vendió \
                            \n o la fecha en el formato dd-mm-aaaa \n')

    #Deduce whether or not payment has been done 
    if fecha_cobro_col == 'no':
        fecha_cobro_col = None
    elif fecha_cobro_col == 'same':
        fecha_cobro_col = fecha_vta_col    
    else:
        date_test(fecha_cobro_col)
        assert( pd.to_datetime(fecha_cobro_col, format='%d-%m-%Y') >= pd.to_datetime(fecha_vta_col, format='%d-%m-%Y') ), 'La fecha de cobro no puede ser anterior a la fecha de venta.'
    
    return fecha_vta_col, fecha_cobro_col

############################################# Sale-filling block 
def filling_sale():
    """ 1. Requires the salesman in charge of the transaction, 
        2. Batch-units quantity pairs and 2.1 generates a dictionary with them,
        3. The total income of the sale,
        4. Calculates the total number of units, price by unit, and sale type.
        5. Finally requires the payment method.

    Returns:
    encargado_col (str): Salesman in charge of transaction
    no_cerv_col (int): Number of units sold
    monto_col (int): Total income
    precio_u_col (float): Unit price
    tipo_col (str): Sale type (12-pack, 6-pack, units)
    pago_col (str): Payment method
    lotes (dict): Dictionary with batch-units number pairs to be injected in the table
    """

    def tipo_generator(no_cerv_col):
        """ Check if the number is divisible by 12, 6 or neither to assign the sale type (12-pack, 6-pack or by unity).

        Args:
        no_cerv_col (int): Number of units by bach 

        Returns: 
        tipo_col (str): Sale type selected by divisibility of the total of the units.
        """
        # 12-pack
        if no_cerv_col % 12 == 0:
            tipo_col = 'doce'
        #6-pack
        elif no_cerv_col == 6:
            tipo_col ='six'
        #unidad
        else: 
            tipo_col = 'unidad'
        return tipo_col    
                
    ################ Salesman in charge column
    lista_encargados = ['A', 'R', 'AR']
    encargado_col = input('Quien realizó la venta? {} \n'.format(lista_encargados))
    assert(encargado_col in lista_encargados), 'El encargado debe estar contenido dentro de las opciones.'

    ################# Batches dictionary
    lote_status = input('Escriba el lote seguido de un espacio y la cantidad de cervezas correspondiente \
    \n e.g. L241221 6 \n')
    
    no_cerv_col = 0
    lotes={}
    while lote_status != 'done':
        lote_col, no_cerv  = lote_status.split() 
        lotes.update({lote_col:no_cerv})
        ############### Number of units
        no_cerv_col = no_cerv_col + int(no_cerv)
        lote_status = input('Ingresa otro lote en el formato requerido o escribe "done" para finalizar \n')

    ############### Total income column
    monto_col = int(input('Monto total ($) \n'))

    ############### unit's price column
    precio_u_col = round(monto_col/no_cerv_col, 2)

    ################# Sale type column
    tipo_col = tipo_generator(no_cerv_col)

    ############### Payment method column
    pago_opciones=['Transf', 'Dep', 'Efec']
    pago_col = input('Tipo de pago. {} \n'.format(pago_opciones))
    assert(pago_col in pago_opciones), 'Por favor escribe una de las opciones de pago.'
        
    return encargado_col, no_cerv_col, monto_col, precio_u_col, tipo_col, pago_col, lotes

def confirmation(new_entry, table, cursor):
    """ Take a list of new values and return a dataframe made with them and the columns of the associated table.
    The function's objetive is to make a visual representation of the final entry ingestion in order to allow
    the user see the whole row before begin the injection to database's table.

    Args:
    new_entry (list): list representing the new row
    table (str): database's table from which columns will be extracted
    cursot (cursor parameter): cursor used by the connection to database

    Returns:
    row_confirm (DataFrame): DataFrame with only the new row
    """
    # Extraemos columnas mediante una dummy row
    query = 'SELECT * FROM {} LIMIT 1;'.format(table)
    cursor.execute(query)

    # creamos las duplas columna-valor 
    cols_list = [i[0] for i in cursor.description]
    
    # creamos el dataframe usando las columnas extraidas por la query y la lista de entrada como nueva fila
    row_confirm = pd.DataFrame([new_entry], columns=cols_list)

    return row_confirm

      



if __name__ == '__main__':
    main()




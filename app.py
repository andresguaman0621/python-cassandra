# from cassandra.cluster import Cluster
# from cassandra.io.asyncorereactor import AsyncoreConnection as DefaultConnection

# cluster = Cluster(['127.0.0.1'], port=9042)
# session = cluster.connect("mercadolibre_test")

# rows = session.execute('SELECT * FROM producto;')
# for producto_row in rows:
#   print(producto_row)


from cassandra.cluster import Cluster
from cassandra.query import dict_factory
import uuid
from datetime import datetime

def connect_to_cassandra():
    cluster = Cluster(['127.0.0.1'], port=9042)
    session = cluster.connect("mercadolibre_test")
    session.row_factory = dict_factory
    return session

def mostrar_menu():
    print("\n--- Menú Principal ---")
    print("1. Operaciones con Productos")
    print("2. Operaciones con Clientes")
    print("3. Operaciones con Pedidos")
    print("4. Salir")

def mostrar_submenu():
    print("\n--- Submenu ---")
    print("1. Crear")
    print("2. Leer")
    print("3. Actualizar")
    print("4. Eliminar")
    print("5. Volver al menú principal")

def crear_producto(session):
    nombre = input("Ingrese el nombre del producto: ")
    categoria = input("Ingrese la categoría del producto: ")
    precio = float(input("Ingrese el precio del producto: "))
    reemplazos = set(input("Ingrese los reemplazos separados por coma: ").split(','))
    
    query = """
    INSERT INTO producto (producto_id, nombre, categoria, precio, reemplazos)
    VALUES (%s, %s, %s, %s, %s)
    """
    session.execute(query, (uuid.uuid4(), nombre, categoria, precio, reemplazos))
    print("Producto creado exitosamente.")

def leer_productos(session):
    rows = session.execute("SELECT * FROM producto")
    for row in rows:
        print(row)


def actualizar_producto(session):
    producto_id = input("Ingrese el ID del producto a actualizar: ")
    nombre = input("Ingrese el nombre actual del producto: ")
    categoria = input("Ingrese la nueva categoría (dejar en blanco para no cambiar): ")
    precio = input("Ingrese el nuevo precio (dejar en blanco para no cambiar): ")
    
    update_parts = []
    params = []
    
    if categoria:
        update_parts.append("categoria = %s")
        params.append(categoria)
    if precio:
        update_parts.append("precio = %s")
        params.append(float(precio))
    
    if update_parts:
        query = f"UPDATE producto SET {', '.join(update_parts)} WHERE producto_id = %s AND nombre = %s"
        params.extend([uuid.UUID(producto_id), nombre])
        session.execute(query, params)
        print("Producto actualizado exitosamente.")
    else:
        print("No se realizaron cambios.")

def eliminar_producto(session):
    producto_id = input("Ingrese el ID del producto a eliminar: ")
    query = "DELETE FROM producto WHERE producto_id = %s"
    session.execute(query, [uuid.UUID(producto_id)])
    print("Producto eliminado exitosamente.")

def crear_cliente(session):
    cliente_identificacion = input("Ingrese la identificación del cliente: ")
    nombre = input("Ingrese el nombre del cliente: ")
    telefono = input("Ingrese el teléfono del cliente: ")
    email = input("Ingrese el email del cliente: ")
    direcciones = set(input("Ingrese las direcciones separadas por coma: ").split(','))
    
    query = """
    INSERT INTO clientes (cliente_id, cliente_identificacion, nombre, telefono, email, direcciones)
    VALUES (%s, %s, %s, %s, %s, %s)
    """
    session.execute(query, (uuid.uuid4(), cliente_identificacion, nombre, telefono, email, direcciones))
    print("Cliente creado exitosamente.")

def leer_clientes(session):
    rows = session.execute("SELECT * FROM clientes")
    for row in rows:
        print(row)

# def actualizar_cliente(session):
#     cliente_id = input("Ingrese el ID del cliente a actualizar: ")
#     nombre = input("Ingrese el nuevo nombre (dejar en blanco para no cambiar): ")
#     telefono = input("Ingrese el nuevo teléfono (dejar en blanco para no cambiar): ")
#     email = input("Ingrese el nuevo email (dejar en blanco para no cambiar): ")
    
#     update_parts = []
#     params = [uuid.UUID(cliente_id)]
    
#     if nombre:
#         update_parts.append("nombre = %s")
#         params.append(nombre)
#     if telefono:
#         update_parts.append("telefono = %s")
#         params.append(telefono)
#     if email:
#         update_parts.append("email = %s")
#         params.append(email)
    
#     if update_parts:
#         query = f"UPDATE clientes SET {', '.join(update_parts)} WHERE cliente_id = %s"
#         session.execute(query, params)
#         print("Cliente actualizado exitosamente.")
#     else:
#         print("No se realizaron cambios.")

def actualizar_cliente(session):
    cliente_id = input("Ingrese el ID del cliente a actualizar: ")
    nombre = input("Ingrese el nombre actual del cliente: ")
    telefono = input("Ingrese el nuevo teléfono (dejar en blanco para no cambiar): ")
    email = input("Ingrese el nuevo email (dejar en blanco para no cambiar): ")
    
    update_parts = []
    params = []
    
    if telefono:
        update_parts.append("telefono = %s")
        params.append(telefono)
    if email:
        update_parts.append("email = %s")
        params.append(email)
    
    if update_parts:
        query = f"UPDATE clientes SET {', '.join(update_parts)} WHERE cliente_id = %s AND nombre = %s"
        params.extend([uuid.UUID(cliente_id), nombre])
        session.execute(query, params)
        print("Cliente actualizado exitosamente.")
    else:
        print("No se realizaron cambios.")

def eliminar_cliente(session):
    cliente_id = input("Ingrese el ID del cliente a eliminar: ")
    query = "DELETE FROM clientes WHERE cliente_id = %s"
    session.execute(query, [uuid.UUID(cliente_id)])
    print("Cliente eliminado exitosamente.")

def crear_pedido(session):
    cliente_id = input("Ingrese el ID del cliente: ")
    productos = {}
    while True:
        producto_id = input("Ingrese el ID del producto (o 'fin' para terminar): ")
        if producto_id.lower() == 'fin':
            break
        cantidad = int(input("Ingrese la cantidad: "))
        productos[uuid.UUID(producto_id)] = cantidad
    
    query = """
    INSERT INTO pedidos (pedido_id, cliente_id, fecha, productos)
    VALUES (%s, %s, %s, %s)
    """
    session.execute(query, (uuid.uuid4(), uuid.UUID(cliente_id), datetime.now(), productos))
    print("Pedido creado exitosamente.")

def leer_pedidos(session):
    rows = session.execute("SELECT * FROM pedidos")
    for row in rows:
        print(row)


def actualizar_pedido(session):
    pedido_id = input("Ingrese el ID del pedido a actualizar: ")
    fecha = input("Ingrese la fecha del pedido (YYYY-MM-DD): ")
    productos = {}
    while True:
        producto_id = input("Ingrese el ID del producto a actualizar (o 'fin' para terminar): ")
        if producto_id.lower() == 'fin':
            break
        cantidad = int(input("Ingrese la nueva cantidad: "))
        productos[uuid.UUID(producto_id)] = cantidad
    
    if productos:
        query = "UPDATE pedidos SET productos = productos + %s WHERE pedido_id = %s AND fecha = %s"
        session.execute(query, [productos, uuid.UUID(pedido_id), datetime.strptime(fecha, "%Y-%m-%d")])
        print("Pedido actualizado exitosamente.")
    else:
        print("No se realizaron cambios.")

def eliminar_pedido(session):
    pedido_id = input("Ingrese el ID del pedido a eliminar: ")
    query = "DELETE FROM pedidos WHERE pedido_id = %s"
    session.execute(query, [uuid.UUID(pedido_id)])
    print("Pedido eliminado exitosamente.")

def main():
    session = connect_to_cassandra()
    
    while True:
        mostrar_menu()
        opcion = input("Seleccione una opción: ")
        
        if opcion == '1':
            while True:
                mostrar_submenu()
                subopcion = input("Seleccione una opción: ")
                if subopcion == '1':
                    crear_producto(session)
                elif subopcion == '2':
                    leer_productos(session)
                elif subopcion == '3':
                    actualizar_producto(session)
                elif subopcion == '4':
                    eliminar_producto(session)
                elif subopcion == '5':
                    break
                else:
                    print("Opción no válida. Intente de nuevo.")
        
        elif opcion == '2':
            while True:
                mostrar_submenu()
                subopcion = input("Seleccione una opción: ")
                if subopcion == '1':
                    crear_cliente(session)
                elif subopcion == '2':
                    leer_clientes(session)
                elif subopcion == '3':
                    actualizar_cliente(session)
                elif subopcion == '4':
                    eliminar_cliente(session)
                elif subopcion == '5':
                    break
                else:
                    print("Opción no válida. Intente de nuevo.")
        
        elif opcion == '3':
            while True:
                mostrar_submenu()
                subopcion = input("Seleccione una opción: ")
                if subopcion == '1':
                    crear_pedido(session)
                elif subopcion == '2':
                    leer_pedidos(session)
                elif subopcion == '3':
                    actualizar_pedido(session)
                elif subopcion == '4':
                    eliminar_pedido(session)
                elif subopcion == '5':
                    break
                else:
                    print("Opción no válida. Intente de nuevo.")
        
        elif opcion == '4':
            print("Gracias por usar el programa. ¡Hasta luego!")
            break
        
        else:
            print("Opción no válida. Intente de nuevo.")

if __name__ == "__main__":
    main()
import pika
from .middleware import MessageMiddlewareQueue, MessageMiddlewareExchange
from .middleware import (
    MessageMiddlewareMessageError,
    MessageMiddlewareDisconnectedError,
    MessageMiddlewareCloseError,
    MessageMiddlewareDeleteError,
    MessageMiddleware
)

class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):

    def __init__(self, host, queue_name):
        try:
            #Creo una conexion TCP, bloqueante, con los parametros pasados
            self.connection = pika.BlockingConnection(pika.ConnectionParameters(host))
            # creo un canal para dirigir comandos a la conexion creada
            self.channel = self.connection.channel()
            #Declaro una cola para su creacion
            self.channel.queue_declare(queue=queue_name) 
            #guardo el identificador de esa cola
            self.queue_name = queue_name 
        except Exception as e:
            raise MessageMiddlewareDisconnectedError(f"Connection error from pika to RabbitMQ.{e}")

	#Envía un mensaje a la cola o al tópico con el que se inicializó el exchange.
	#Si se pierde la conexión con el middleware eleva MessageMiddlewareDisconnectedError.
	#Si ocurre un error interno que no puede resolverse eleva MessageMiddlewareMessageError.
    def send(self, message):
        try:
            #Envio el mensaje en el body a la cola del identificador routing_key
            self.channel.basic_publish(exchange="",routing_key=self.queue_name,body=message)
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(f"Connection to middleware lost in SEND{e}")
        except  pika.exceptions.AMQPError as e:
            raise MessageMiddlewareMessageError(f"Internal Error {e}")


	#Comienza a escuchar a la cola/exchange e invoca a on_message_callback tras
	#cada mensaje de datos o de control con el cuerpo del mensaje.
	# on_message_callback tiene como parámetros:
	# message - El valor tal y como lo recibe el método send de esta clase.
	# ack - Función que al invocarse realiza ack al mensaje que se está consumiendo.
	# nack - Función que al invocarse realiza nack al mensaje que se está consumiendo. 
	#Si se pierde la conexión con el middleware eleva MessageMiddlewareDisconnectedError.
	#Si ocurre un error interno que no puede resolverse eleva MessageMiddlewareMessageError.
    def start_consuming(self, on_message_callback):
        try:
            #creo la funcion pedida por pika para poder procesar lo mensajes
            # indicando que se llama para realizar el ack o el nack
            # en ambos casos encapsulamos los metodos de basik_ack y basic_nack para esto
            # y ejecutamos el callback externo para ejecutar logica de usuario
            def on_message(channel,method,properties, body):
                def ack():
                    channel.basic_ack(delivery_tag=method.delivery_tag)
                def nack():
                    channel.basic_nack(delivery_tag=method.delivery_tag)
                on_message_callback(body,ack,nack)
            # Indicamos cual es el objetivo donde deben llegar los mensajes, indicando que sea la cola creada
            # luego colocamos el chanel a escuchar hasta que reciba un evento de red
            self.channel.basic_consume(queue=self.queue_name,on_message_callback=on_message)
            self.channel.start_consuming()
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(f"Connection to middleware lost cant start consuming{e}")
        except pika.exceptions.AMQPError as e:
            raise MessageMiddlewareMessageError(f"Internal Error {e}")

    #Si se estaba consumiendo desde la cola/exchange, se detiene la escucha. Si
	#no se estaba consumiendo de la cola/exchange, no tiene efecto, ni levanta
	#Si se pierde la conexión con el middleware eleva MessageMiddlewareDisconnectedError.
    def stop_consuming(self):
        try:
            # Interrumpimos el estado de estar escuchando eventos, sin cerrar ninguna conexion
            self.channel.stop_consuming()
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(f"Connection to middleware lost cant stop consuming {e}")

    #Se desconecta de la cola o exchange al que estaba conectado.
	#Si ocurre un error interno que no puede resolverse eleva MessageMiddlewareCloseError.
    def close(self):
        try:
            #Le indicamos al channel que debe cerrarse sin romper la conexion,  
            #luego cerramos la conexion para liberar correctamente los recursos
            self.channel.close()
            self.connection.close()
        except pika.exceptions.AMQPError as e:
            raise MessageMiddlewareCloseError(f"Internal Error {e}")

class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):
    
    def __init__(self, host, exchange_name, routing_keys):
        try:
            # creamos un exchange, nos quedamos con la lsita de topics y el nombre para referenciarlo
            self.exchange_name = exchange_name
            self.routing_keys = routing_keys
            # creamos una conexion tcp bloqueante con los parametros pasados
            #creamos un channel para esa conexion
            self.connection = pika.BlockingConnection(parameters=pika.ConnectionParameters(host=host))
            self.channel = self.connection.channel()
            # Declaramos el exchange con el nombre dado
            self.exchange = self.channel.exchange_declare(exchange=self.exchange_name, exchange_type='direct')
            self.queue_name = None
            
        except Exception as e:
                    raise MessageMiddlewareDisconnectedError(f"Creation of exchange failed.{e}")

    #Envía un mensaje a la cola o al tópico con el que se inicializó el exchange.
	#Si se pierde la conexión con el middleware eleva MessageMiddlewareDisconnectedError.
	#Si ocurre un error interno que no puede resolverse eleva MessageMiddlewareMessageError.
    def send(self, message):
        try:
            # iteramos todas las routing keys para redireccionar el paquete, de esta forma
            # cada envio se hace con una etiqueta diferente
            for key in self.routing_keys:
                self.channel.basic_publish(exchange=self.exchange_name,routing_key=key,body=message)

        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(f"Connection to middleware lost in SEND {e}")
        except pika.exceptions.AMQPError as e:
            raise MessageMiddlewareMessageError(f"Internal Error {e}")

    
	#Comienza a escuchar a la cola/exchange e invoca a on_message_callback tras
	#cada mensaje de datos o de control con el cuerpo del mensaje.
	# on_message_callback tiene como parámetros:
	# message - El valor tal y como lo recibe el método send de esta clase.
	# ack - Función que al invocarse realiza ack al mensaje que se está consumiendo.
	# nack - Función que al invocarse realiza nack al mensaje que se está consumiendo. 
	#Si se pierde la conexión con el middleware eleva MessageMiddlewareDisconnectedError.
	#Si ocurre un error interno que no puede resolverse eleva MessageMiddlewareMessageError.
    def start_consuming(self, on_message_callback):
        try:
            # creamos una cola temporal, pidiendo un nombre aleatorio e indicando que solo viva
            # mientras viva la conexion
            self.queue_name = self.channel.queue_declare(queue='',exclusive=True).method.queue
            # Iteramos las keys y bindeamos a la cola temporal con el exchange por cada key
            # generando un puente entre ambas por cada key
            for key in self.routing_keys:
                self.channel.queue_bind(queue=self.queue_name,exchange=self.exchange_name,routing_key=key)

            #creo la funcion pedida por pika para poder procesar lo mensajes
            # indicando que se llama para realizar el ack o el nack
            # en ambos casos encapsulamos los metodos de basik_ack y basic_nack para esto
            # y ejecutamos el callback externo para ejecutar logica de usuario
            def on_message(channel,method,properties,body):
                def ack():
                    channel.basic_ack(delivery_tag=method.delivery_tag)
                def nack():
                    channel.basic_nack(delivery_tag=method.delivery_tag)
                on_message_callback(body,ack,nack)
            self.channel.basic_consume(queue=self.queue_name,on_message_callback=on_message)
            self.channel.start_consuming()
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(f"Connection to middleware lost cant start consuming {e}")
        except pika.exceptions.AMQPError as e:
            raise MessageMiddlewareMessageError(f"Internal Error {e}")

    #Si se estaba consumiendo desde la cola/exchange, se detiene la escucha. Si
	#no se estaba consumiendo de la cola/exchange, no tiene efecto, ni levanta
	#Si se pierde la conexión con el middleware eleva MessageMiddlewareDisconnectedError.
    def stop_consuming(self):
        try:
            # Interrumpimos el estado de estar escuchando eventos, sin cerrar ninguna conexion
            self.channel.stop_consuming()
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(f"Connection to middleware lost cant stop consuming {e}")

    #Se desconecta de la cola o exchange al que estaba conectado.
	#Si ocurre un error interno que no puede resolverse eleva MessageMiddlewareCloseError.
    def close(self):
        try:
            #Le indicamos al channel que debe cerrarse sin romper la conexion,  
            #luego cerramos la conexion para liberar correctamente los recursos
            self.channel.close()
            self.connection.close()
        except pika.exceptions.AMQPError as e:
            raise MessageMiddlewareCloseError(f"Internal Error {e}")
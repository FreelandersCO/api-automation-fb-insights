# Havas Facebook Insghts

Archivo para descripción de configuración

## Para instalar el sdk de python fb

```sh
pip install facebook-sdk
```


## Modelo de base de datos

![Alt text](database/Model.png?raw=true "Modelo")

[Ver detalle](database/Model.png?raw=true)

## SQL

El archivo .sql de el modelo se encuentra en :

[squema.sql](database/squema.sql)

## Configuración de datos

### Tabla page

Los datos a almacenar en la tabla page son las credenciales proporcionadas por facebook

| Dato | Campo |
| ------ | ------ |
| Nombre de la página | name | 
| Token de la página  | token |
| id de página | id_page |
| Estado en el cual se encuentra la tarea (P, R, F) para que se ejecute debe estar en estado P | status |
| Fecha en la cual se ejecuta la tarea | date_run_start |
| Fecha cuando acaba de ejecutar la tarea | date_run_end |


#### Nota

Para consultar las metricas de insight revisar el siguiente link [Avail Metrics]


## Conexión

Para modificar los datos de conexión se debe hacer en el archivo `datab.py` en las variables

| Variable | Dato|
| ------ | ------ |
| user| postgres |
| password | 1nt3r4ct1v3 |
| db | havas_automation |
| host | localhost |
| port | 5432|

## Cambio de motor de base de datos

Para modificar el motor de base de datos se debe modificar la siguiente linea en el archivo `datab.py` 
```python
url = 'postgresql://{}:{}@{}:{}/{}'
```

## Ejecución Data (Pages , Post)

Para realizar la ejecución se debe correr el siguiente comando

```sh
$ python get-data.py
```

## Ejecución Data (Conversation)

Para realizar la ejecución se debe correr el siguiente comando

```sh
$ python conversation.py
```

[Avail Metrics]: <https://developers.facebook.com/docs/graph-api/reference/v3.3/insights#availmetrics> 
# FCI T+0 Comparaciones
Este proyecto genera una web con base en la información de la planilla diaria de CAFCI vs. Inflaciones y CAR populares
## Instrucciones
- pip install - requirements.txt
- Descargar la ultima planilla diaria de [CAFCI](https://www.cafci.org.ar/index.html)
- Renombrar el archivo a fcis (suponiendo que se mantiene .xlsx como extension)
- Setear la variable de entorno [FRED_API_KEY](https://fred.stlouisfed.org/docs/api/api_key.html)
- correr main.py
- main.py sobreescribe index.html solo servir desde un web-server de estáticos

### Aclaraciones
El script de python modifica a T+0 un par de FCI de Cocos. En CAFCI se marcan como T+1, pero en la práctica son T+0
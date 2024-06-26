import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error, r2_score
import matplotlib.pyplot as plt

df_2022 = pd.read_excel('bomberos/ActuacionesBomberos_2022.xlsx')
df_2023 = pd.read_excel('bomberos/ActuacionesBomberos_2023.xlsx')
df_2024 = pd.read_excel('bomberos/ActuacionesBomberos_2024.xlsx')

# Combinar los DataFrames utilizando pd.concat()
df = pd.concat([df_2022, df_2023, df_2024], ignore_index=True)

# Agrupar por 'Departamento' y sumar los salarios
df_agrupado = df.groupby(['AÑO', 'MES'])['TOTAL'].sum().reset_index()
print("\nDataFrame con la suma de accidentes x año, mes:")
print(df_agrupado)


# Seleccionar las características (X) y el objetivo (y)
X = df_agrupado[['AÑO']]
y = df_agrupado['TOTAL']

# Dividir los datos en conjuntos de entrenamiento y prueba
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Crear el modelo de regresión lineal
modelo = LinearRegression()

# Entrenar el modelo
modelo.fit(X_train, y_train)

# Hacer predicciones en el conjunto de prueba
y_pred = modelo.predict(X_test)

# Evaluar el modelo
mse = mean_squared_error(y_test, y_pred)
r2 = r2_score(y_test, y_pred)

print("Mean Squared Error:", mse)
print("R-squared:", r2)

# Mostrar los coeficientes del modelo
print("Intercepto:", modelo.intercept_)
print("Coeficiente:", modelo.coef_)

# Graficar los resultados
plt.scatter(X_test, y_test, color='blue', label='Datos reales')
plt.plot(X_test, y_pred, color='red', linewidth=2, label='Predicciones')
plt.xlabel('AÑO')
plt.ylabel('TOTAL')
plt.title('Regresión Lineal: Edad vs Salario')
plt.legend()
plt.grid(True)
plt.show()
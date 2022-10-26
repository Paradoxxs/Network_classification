import tensorflow as tf
from tensorflow import keras
from tensorflow.keras import layers
import pandas as pd
import numpy as np
from sklearn.preprocessing import LabelEncoder
from sklearn.model_selection import train_test_split

df = pd.read_csv("/content/drive/MyDrive/sample/combined.csv")

classfication_filters = ["label","bidirectional_duration_ms","dst2src_bytes","src2dst_bytes","protocol"]
df = df.loc[:,classfication_filters]
df.head()

X = df.iloc[:,1:].values
y = df.iloc[:,0].values

print(X)
print(y)

encoder =  LabelEncoder()
y1 = encoder.fit_transform(y)
Y = pd.get_dummies(y1).values

x_train, x_test, y_train, y_test = train_test_split(X,Y, test_size=0.2, random_state = 4)

from sklearn.tree import DecisionTreeClassifier
tree = DecisionTreeClassifier()
tree.fit(x_train,y_train)

predictions = tree.predict(x_test)

def evaluate(y,predict):
  results = np.array([y[i] == predict[i] for i, _ in enumerate(y)])
  print(f'accuracy: {results.mean() * 100.0}%')

evaluate(y_test,predictions)


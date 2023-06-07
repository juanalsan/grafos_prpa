import sys
from pyspark import SparkContext


def mapper(line):
    edge = line.strip().split(',')
    n1  = edge[0]
    n2  = edge[1]

    if n1 < n2:
        return (n1, n2) 
    elif n1 > n2:         
        return (n2, n1)    
    else:
        pass 


     
def distintas(spc, filename):
    """
Recibe el fichero,calcula los vértices de cada línea y devuelve los elementos no repetidos
    """
    return spc.textFile(filename).map(mapper).filter(lambda x: x is not None).distinct() 



def pista(adyacencia):
    """
Función que recomienda el apartado 2 del enunciado
    """
    conexiones = []
    for i in range(len(adyacencia[1])):
        conexiones.append(((adyacencia[0], adyacencia[1][i]), 'exists')) 

        for j in range(i+1,len(adyacencia[1])):
            n1 = adyacencia[1][i]
            n2 = adyacencia[1][j]
            v1 = min(n1,n2)
            v2 = min(n1,n2)
            conexiones.append(((v1,v2),('pending',adyacencia[0])))

    return conexiones


  
def condicion(adyacencia):
    return (len(adyacencia[1]) >= 2 and 'exists' in adyacencia[1])


   
def posibles_triciclos(adyacencia):
    """
Calcula los posibles triciclos
    """
    posib = []
    for elem in adyacencia[1]:
        if elem != 'exists':
            posib.append((adyacencia[1], adyacencia[0][0], adyacencia[0][1]))

    return posib


    
def ejercicio2_grafos(spc, filenames):
    """
calcula los 3-ciclos de un grafo definido como lista de aristas en múltiples ficheros

    """
    rdd = spc.textFile(",".join(filenames))      
    aristas = rdd.map(distintas).filter(lambda x: x is not None).distinct()
    conectados = aristas.groupByKey().mapValues(list).flatMap(pista)
    triciclos = conectados.groupByKey().mapValues(list).filter(condicion).flatMap(posibles_triciclos)

    print(triciclos.collect())
    return triciclos.collect()

if __name__ == "__main__":
    
    if len(sys.argv) <= 2:
        print(f"Usage: python3 {0} <filenames>")

    else:
        spc = SparkContext()
        filenames = sys.argv[1:] 
        ejercicio2_grafos(spc, filenames) 

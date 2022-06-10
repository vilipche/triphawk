from pyspark.sql import Row

def appendRowWithDictionary(a: Row, b: dict):
    c = a.asDict()
    c.update(b)
    return c

def putIdInside(id, array, keyName: str):
    new = []
    for row in array:
        updatedrow = Row(**appendRowWithDictionary(row, {keyName: id.oid}))
        new.append(updatedrow)
    return new 

def updateCategories(row):
    res = [i.title for i in row.categories]
    newRow = Row(**appendRowWithDictionary(row, {"categories": res}))
    return newRow
    
def concatenateAddress(a1: str, a2: str, a3: str):
    return f"{a1} {a2} {a3}".rstrip()

def updateAddress(row):
    loc = row.location
    res = concatenateAddress(loc.address1, loc.address2, loc.address3)
    newRow = Row(**appendRowWithDictionary(row.location, {"address": res}))
    return newRow

def updateLocation(row, locationRow):
    return Row(**appendRowWithDictionary(row, {'location': locationRow}))

def removeDataColumns(row, columnsFirst: str, columnsLocation: str):
    dict = row.asDict(True) #recursive
    for col in columnsFirst:
        dict.pop(col)
    
    for col in columnsLocation:
        dict['location'].pop(col)
    return Row(**dict)

def lineage_restaurant(restaurantRDD):

    # put city array inside the data
    innerRDD = restaurantRDD\
                .map(lambda x: (x._id, x.data)) \
                .map(lambda x: putIdInside(x[0], x[1], 'city_id')).flatMap(lambda x:x)

    minBusinessRDD = innerRDD\
                        .map(lambda x: (x.id, x.distance))\
                        .groupByKey()\
                        .mapValues(min)

    cityInBusinessRDD = restaurantRDD\
                            .map(lambda x: (x._id, x.data))\
                            .map(lambda x: putIdInside(x[0], x[1], 'city_id'))\
                            .flatMap(lambda x: x).map(lambda x: (x.id, x))


    # find the right businesses for each neighborhood
    businessByDistanceRDD = minBusinessRDD\
        .join(cityInBusinessRDD)\
        .filter(lambda x: x[1][0] == x[1][1].distance)\
        .map(lambda x: (x[0], x[1][1]))\
        .map(lambda x: (x[0], updateCategories(x[1]))) \
        .map(lambda x: (x[0], updateLocation(x[1], updateAddress(x[1])) )) \
        .map(lambda x: (x[0], removeDataColumns(x[1], ['alias', 'display_phone', 'distance', 'is_closed'], ['address1', 'address2', 'address3', 'state', 'display_address' ]) )) 

    businessesRDD = businessByDistanceRDD\
                        .map(lambda x: (x[1].city_id, x[1]))\
                        .groupByKey()\
                        .mapValues(list)

    firstLevelRDD = restaurantRDD.map(lambda x: (x._id.oid, (x.location, x.type)))

    finalRDD = businessesRDD.join(firstLevelRDD).map(lambda x: Row(_id=x[0],location=x[1][1][0], type=x[1][1][1], data=x[1][0]))

    return finalRDD

def lineage_bar(barRDD):
    return lineage_restaurant(barRDD)

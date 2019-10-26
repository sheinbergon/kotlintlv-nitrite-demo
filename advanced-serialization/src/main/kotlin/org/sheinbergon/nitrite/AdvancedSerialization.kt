package org.sheinbergon.nitrite

import MyDataNitriteMapper
import org.dizitart.kno2.emptyDocument
import org.dizitart.kno2.filters.eq
import org.dizitart.kno2.filters.within
import org.dizitart.no2.Nitrite
import org.dizitart.no2.UpdateOptions
import org.dizitart.no2.objects.Id
import kotlin.reflect.KProperty1

enum class Value { ONE, TWO, THREE }

data class MyData(@Id val id: String, val value: Value, val flag: Boolean, val optional: Int? = null)

object MyDataRepository {

    private val nitrite = Nitrite.builder()
        .nitriteMapper(MyDataNitriteMapper)
        .openOrCreate()

    private val repository = nitrite.getRepository(MyData::class.java)

    private val collection = repository.documentCollection

    fun insert(data: MyData) {
        repository.insert(data)
    }

    fun get(id: String): MyData? = repository
        .find(MyData::id eq id)
        .let { cursor -> runCatching { cursor.first() } }
        .getOrNull()

    fun get(values: Collection<Value>): List<MyData> = repository
        .find(MyData::value within values)
        .let { cursor -> runCatching { cursor.toList() } }
        .getOrNull()
        .orEmpty()

    fun update(id: String, fields: Map<KProperty1<MyData, *>, Any>) {
        val document = emptyDocument()
        fields.forEach { (field, value) -> document[field.name] = MyDataNitriteMapper.asSerialized(value) }
        document[MyData::id.name] = id
        collection.update(MyData::id.name eq id, document, UpdateOptions.updateOptions(true))
    }
}

fun main() {
    val datum1 = MyData("id-1", Value.ONE, true)
    val datum2 = MyData("id-2", Value.THREE, true, 5)
    MyDataRepository.insert(datum1)
    MyDataRepository.insert(datum2)
    println(MyDataRepository.get("id-1"))
    println(MyDataRepository.get(listOf(Value.THREE)))
    MyDataRepository.update("id-1", mapOf(MyData::value to Value.THREE, MyData::optional to 10))
    println(MyDataRepository.get(listOf(Value.THREE)))
}
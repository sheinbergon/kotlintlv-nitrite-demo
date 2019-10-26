package org.sheinbergon.nitrite

import org.dizitart.kno2.emptyDocument
import org.dizitart.kno2.filters.eq
import org.dizitart.no2.Nitrite
import org.dizitart.no2.UpdateOptions
import org.dizitart.no2.objects.Id
import kotlin.reflect.KProperty
import kotlin.reflect.KProperty1

data class MyData(@Id val id: String, val number: Int, val flag: Boolean)

object MyDataRepository {

    private val nitrite = Nitrite.builder().openOrCreate()

    private val repository = nitrite.getRepository(MyData::class.java)

    private val collection = repository.documentCollection

    fun upsert(data: MyData) {
        repository.update(data, true)
    }

    fun update(id: String, fields: Map<KProperty1<MyData, *>, Any>) {
        val document = emptyDocument()
        fields.forEach { (field, value) -> document[field.name] = value }
        document[MyData::id.name] = id
        val options = UpdateOptions.updateOptions(true)
        collection.update(MyData::id.name eq id, document, options)
    }

    fun get(id: String): MyData? = repository
        .find(MyData::id eq id)
        .let { cursor -> runCatching { cursor.first() } }
        .getOrNull()
}

fun main() {
    val datum1 = MyData("id-1", 5, true)
    MyDataRepository.upsert(datum1)
    println(MyDataRepository.get("id-1"))
    val datum2 = MyData("id-1", 2, false)
    MyDataRepository.upsert(datum2)
    println(MyDataRepository.get("id-1"))
    MyDataRepository.update("id-1", mapOf(MyData::number to 22))
    println(MyDataRepository.get("id-1"))
}
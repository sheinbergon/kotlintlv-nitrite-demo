package org.sheinbergon.nitrite

import org.dizitart.kno2.filters.gt
import org.dizitart.no2.IndexOptions
import org.dizitart.no2.IndexType
import org.dizitart.no2.Nitrite
import org.dizitart.no2.objects.Id

data class MyData(@Id val id: String, val number: Int, val flag: Boolean)

object MyDataRepository {

    private val nitrite = Nitrite.builder().openOrCreate()

    private val repository = nitrite.getRepository(MyData::class.java)

    fun insert(data: MyData) {
        repository.insert(data)
    }

    init {
        repository.createIndex("number", IndexOptions.indexOptions(IndexType.NonUnique))
    }

    fun greaterThan(number: Int): List<MyData> = repository
        .find(MyData::number gt number)
        .let { kotlin.runCatching { it.toList() } }
        .getOrElse { emptyList() }
}

fun main() {
    val datum1 = MyData("id-1", 5, true)
    val datum2 = MyData("id-2", 12, false)
    MyDataRepository.insert(datum1)
    MyDataRepository.insert(datum2)
    println(MyDataRepository.greaterThan(2).size)
    println(MyDataRepository.greaterThan(6).size)
}
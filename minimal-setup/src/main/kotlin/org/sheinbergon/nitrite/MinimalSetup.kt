package org.sheinbergon.nitrite

import org.dizitart.kno2.filters.eq
import org.dizitart.no2.Nitrite
import org.dizitart.no2.objects.Id
import org.dizitart.no2.objects.filters.ObjectFilters

data class MyData(@Id val id: String, val number: Int, val flag: Boolean)

object MyDataRepository {

    private val nitrite = Nitrite.builder().openOrCreate()

    private val repository = nitrite.getRepository(MyData::class.java)

    fun insert(data: MyData) {
        repository.insert(data)
    }

    fun get(id: String): MyData? = repository
        .find(MyData::id eq id)
        .let { cursor -> runCatching { cursor.first() } }
        .getOrNull()

    fun all(): List<MyData> = repository
        .find(ObjectFilters.ALL)
        .toList()
}

fun main() {
    val datum1 = MyData("id-1", 5, true)
    MyDataRepository.insert(datum1)
    println(MyDataRepository.all().size)
    val datum2 = MyData("id-2", 12, false)
    MyDataRepository.insert(datum2)
    println(MyDataRepository.get("id-2")?.number)
    println(MyDataRepository.get("id-3") ?: "id-3 not found")
}
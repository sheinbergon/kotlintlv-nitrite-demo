package org.sheinbergon.nitrite

import org.dizitart.kno2.filters.and
import org.dizitart.kno2.filters.eq
import org.dizitart.kno2.filters.or
import org.dizitart.no2.IndexOptions
import org.dizitart.no2.IndexType
import org.dizitart.no2.Nitrite
import org.dizitart.no2.objects.Id
import org.dizitart.no2.objects.filters.ObjectFilters

data class MyData(@Id val id: String, val flag: Boolean, val nested: MyNestedData)

data class MyNestedData(val criteria: Int)

object MyDataRepository {

    private val nitrite = Nitrite.builder().openOrCreate()

    private val repository = nitrite.getRepository(MyData::class.java)

    init {
        repository.createIndex("flag", IndexOptions.indexOptions(IndexType.NonUnique))
        repository.createIndex("nested.criteria", IndexOptions.indexOptions(IndexType.NonUnique))
    }

    fun insert(vararg data: MyData) {
        repository.insert(data)
    }

    fun on(number: Int, flag: Boolean): List<MyData> {
        val filter = ObjectFilters.eq("nested.criteria", 0) or
                (ObjectFilters.gt("nested.criteria", number) and (MyData::flag eq flag))
        return repository
            .find(filter).let { kotlin.runCatching { it.toList() } }
            .getOrElse { emptyList() }
    }
}

fun main() {
    val datum1 = MyData("id-1", true, MyNestedData(5))
    val datum2 = MyData("id-2", false, MyNestedData(21))
    val datum3 = MyData("id-3", false, MyNestedData(6))
    val datum4 = MyData("id-4", false, MyNestedData(12))
    val datum5 = MyData("id-5", false, MyNestedData(0))
    MyDataRepository.insert(datum1, datum2, datum3, datum4, datum5)
    println(MyDataRepository.on(1, true).map(MyData::id))
    println(MyDataRepository.on(10, false).map(MyData::id))
}
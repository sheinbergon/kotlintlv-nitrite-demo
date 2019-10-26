package org.sheinbergon.nitrite

import org.dizitart.kno2.filters.eq
import org.dizitart.no2.Nitrite
import org.dizitart.no2.objects.Id

data class MyData(@Id val id: String, val number: Int, val flag: Boolean)

data class ProjectedData(val id: String, val number: Int)

object MyDataRepository {

    private val nitrite = Nitrite.builder().openOrCreate()

    private val repository = nitrite.getRepository(MyData::class.java)

    fun insert(data: MyData) {
        repository.insert(data)
    }

    fun <P> get(id: String, target: Class<P>): P? = repository
        .find(MyData::id eq id)
        .project(target)
        .let { kotlin.runCatching { it.firstOrDefault() } }
        .getOrNull()
}

fun main() {
    val datum1 = MyData("id-1", 5, true)
    MyDataRepository.insert(datum1)
    println(MyDataRepository.get("id-1", ProjectedData::class.java))
}
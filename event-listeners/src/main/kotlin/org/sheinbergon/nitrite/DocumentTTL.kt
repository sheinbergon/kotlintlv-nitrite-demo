package org.sheinbergon.nitrite

import org.dizitart.kno2.filters.eq
import org.dizitart.no2.Nitrite
import org.dizitart.no2.event.ChangeInfo
import org.dizitart.no2.event.ChangeListener
import org.dizitart.no2.event.ChangeType
import org.dizitart.no2.objects.Id
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

data class MyData(@Id val id: String, val number: Int?, val flag: Boolean)

object MyDataRepository {

    private const val TTL = 500L
    private val nitrite = Nitrite.builder().openOrCreate()
    private val scheduler = Executors.newSingleThreadScheduledExecutor()

    private val repository = nitrite.getRepository(MyData::class.java).also {
        it.register(object : ChangeListener {
            override fun onChange(info: ChangeInfo) = info.changedItems
                .filter { it.changeType == ChangeType.INSERT }
                .forEach {
                    val id = it.document["id", String::class.java]
                    scheduler.schedule({ remove(id) }, TTL, TimeUnit.MILLISECONDS)
                }
        })
    }

    fun insert(data: MyData) {
        repository.insert(data)
    }

    fun remove(id: String) {
        repository.remove(MyData::id eq id)
    }

    fun get(id: String): MyData? = repository
        .find(MyData::id eq id)
        .let { cursor -> runCatching { cursor.first() } }
        .getOrNull()
}

fun main() {
    val datum1 = MyData("id-1", 5, true)
    MyDataRepository.insert(datum1)
    println(MyDataRepository.get("id-1"))
    Thread.sleep(550L)
    println(MyDataRepository.get("id-1"))
}
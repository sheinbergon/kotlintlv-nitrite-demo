import org.dizitart.kno2.documentOf
import org.dizitart.no2.Document
import org.dizitart.no2.mapper.JacksonMapper
import org.sheinbergon.nitrite.MyData
import org.sheinbergon.nitrite.Value

internal object MyDataNitriteMapper : JacksonMapper() {

    override fun isValueType(`object`: Any?) = when (`object`) {
        is Enum<*> -> true // Enums are are mapped as value types using their names
        else -> super.isValueType(`object`)
    }

    override fun asValue(`object`: Any?) = when (`object`) {
        is Enum<*> -> `object`.name
        else -> super.asValue(`object`)
    }

    private fun asMyDataObject(document: Document): MyData = MyData(
        id = document["id", String::class.java],
        flag = document["flag", java.lang.Boolean::class.java].booleanValue(),
        value = Value.valueOf(document["value", String::class.java]),
        optional = document["optional", Integer::class.java]?.toInt()
    )

    override fun <T> asObject(document: Document, type: Class<T>) = when (type) {
        MyData::class.java -> asMyDataObject(document)
        else -> asObjectInternal(document, type)
    } as T

    private fun asMyDataDocument(data: MyData) = documentOf(
        "id" to data.id,
        "flag" to data.flag,
        "value" to data.value.name
    ).also { document ->
        data.optional?.let { document["optional"] = it }
    }

    override fun <T : Any?> asDocument(`object`: T) = when (`object`) {
        is MyData -> asMyDataDocument(`object`)
        else -> asDocumentInternal(`object`)
    }

    fun asSerialized(`object`: Any): Any = `object`
        .takeIf(this::isValueType)
        ?.let(this::asValue)
        ?: this.asDocument(`object`)
}
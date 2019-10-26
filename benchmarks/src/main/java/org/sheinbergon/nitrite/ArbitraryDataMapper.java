package org.sheinbergon.nitrite;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.val;
import org.dizitart.no2.Document;
import org.dizitart.no2.mapper.JacksonMapper;
import org.dizitart.no2.mapper.NitriteMapper;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class ArbitraryDataMapper extends JacksonMapper {

    public final static NitriteMapper INSTANCE = new ArbitraryDataMapper();

    @Override
    public <T> T asObjectInternal(Document document, Class<T> type) {
        if (type.equals(ArbitraryData.class)) return (T) asAribtraryDataObject(document);
        else return asObjectInternal(document, type);
    }

    @Override
    public <T> Document asDocument(T object) {
        if (object instanceof ArbitraryData) return asAribtraryDataDocument((ArbitraryData)object);
        else return asDocumentInternal(object);
    }

    private ArbitraryData asAribtraryDataObject(Document document) {
        return new ArbitraryData()
                .id(document.get("id", Integer.class))
                .text(document.get("text", String.class))
                .number1(document.get("number1", Double.class))
                .number2(document.get("number2", Double.class))
                .index1(document.get("index1", Integer.class))
                .flag1(document.get("flag1", Boolean.class))
                .flag2(document.get("flag2", Boolean.class));
    }


    private Document asAribtraryDataDocument(ArbitraryData datum) {
        val document = new Document();
        document.put("id", datum.id());
        document.put("text", datum.text());
        document.put("number1", datum.number1());
        document.put("number2", datum.number2());
        document.put("index1", datum.index1());
        document.put("flag1", datum.flag1());
        document.put("flag2", datum.flag2());
        return document;
    }
}

/*
     Функция, при помощи которой мы забираем данные из API Яндекс.Вебмастер
     Свой токен можно узнать тут: https://oauth.yandex.ru/authorize?response_type=token&client_id=f08ac1790cc9409aa328b3eda091d105

     Версия 1.2

     Changelog:
     1.1. Добавил проверку на подтвержденность прав Яндекс.Вебмастер
     1.2. Проверка на домены без данных с удалением ошибок
     Создатель: Эльдар Забитов (http://zabitov.ru)
*/


let
    searchQueries = (token as text, orderby as text) =>
    let
        authKey = "OAuth "&token,
        //Получаем UserID
        url = "https://api.webmaster.yandex.net/v3/user/",
        userIdSource = Web.Contents(url,
            [Headers = [#"Authorization"=authKey]]),

        //Перекладываем UserID в таблицу и достаем значение
            getUserId = Json.Document(userIdSource,1251),
            userIdToTable = Record.ToTable(getUserId),
            typeToText = Table.TransformColumnTypes(userIdToTable,{{"Value", type text}}),
            userId = typeToText{0}[Value],

        //Получаем список сайтов в аккаунте и кладем в массив
        getSiteListSource = Web.Contents(url&userId&"/hosts/",
            [Headers = [#"Authorization"=authKey]]),
            jsonList = Json.Document(getSiteListSource,1251),
            hosts = jsonList[hosts],
            hostToTable = Table.FromList(hosts, Splitter.SplitByNothing(), null, null, ExtraValues.Error),
            tableExpose = Table.ExpandRecordColumn(hostToTable, "Column1", {"host_id", "unicode_host_url", "verified"}, {"host_id", "unicode_host_url", "verified"}),
            filterVerified = Table.SelectRows(tableExpose, each ([verified] = true)),
            deleteVerifiedColumn = Table.RemoveColumns(filterVerified,{"verified"}),


        //Генерим функцию
        getQueriesFn = (hostId as text) =>
        let
            getQuerySource =  Web.Contents(url & userId & "/hosts/"& hostId & "/search-queries/popular/?order_by="&orderby&"&query_indicator=TOTAL_SHOWS&query_indicator=TOTAL_CLICKS&query_indicator=AVG_SHOW_POSITION&query_indicator=AVG_CLICK_POSITION",
            [Headers = [#"Authorization"=authKey]]),
            jsonListquery = Json.Document(getQuerySource,65001),
            listOfQueryToTable = Record.ToTable(jsonListquery),
            transpot = Table.Transpose(listOfQueryToTable),
            promoteHeaders = Table.PromoteHeaders(transpot),
            expandColumn = Table.ExpandListColumn(promoteHeaders, "queries"),
            expandColumn2 = Table.ExpandRecordColumn(expandColumn, "queries", {"query_id", "query_text", "indicators"}, {"query_id", "query_text", "indicators"}),
            expandColumn3 = Table.ExpandRecordColumn(expandColumn2, "indicators", {"TOTAL_SHOWS", "TOTAL_CLICKS", "AVG_SHOW_POSITION", "AVG_CLICK_POSITION"}, {"TOTAL_SHOWS", "TOTAL_CLICKS", "AVG_SHOW_POSITION", "AVG_CLICK_POSITION"}),
            combineDates = Table.CombineColumns(expandColumn3,{"date_from", "date_to"},Combiner.CombineTextByDelimiter(" — ", QuoteStyle.None),"Период")
        in
            combineDates,

        //Используем функцию с host_id в виде аргумента
        getAllHosts = Table.AddColumn(deleteVerifiedColumn, "Custom", each getQueriesFn([host_id])),
        removeErrors = Table.RemoveRowsWithErrors(getAllHosts, {"Custom"}),
        expandToFinal = Table.ExpandTableColumn(removeErrors, "Custom", {"query_id", "query_text", "TOTAL_SHOWS", "TOTAL_CLICKS", "AVG_SHOW_POSITION", "AVG_CLICK_POSITION", "Период"}, {"query_id", "query_text", "TOTAL_SHOWS", "TOTAL_CLICKS", "AVG_SHOW_POSITION", "AVG_CLICK_POSITION", "Период"}),
        deleteHostId = Table.RemoveColumns(expandToFinal,{"host_id"}),
            //Запускаем R-скрипт
            //Не забудьте поправить путь к файлу
            R = R.Execute("require(gdata)#(lf)print(Sys.getlocale(category = ""LC_CTYPE""))#(lf)original_ctype <- Sys.getlocale(category = ""LC_CTYPE"")#(lf)Sys.setlocale(""LC_CTYPE"",""japanese"")#(lf)write.table(trim(dataset), file=""C:/Users/margerko/Desktop/New folder/1111.txt"", sep = ""\t"", row.names = FALSE, fileEncoding = ""UTF-8"", append=TRUE, col.names = FALSE)#(lf)plot(dataset);",[dataset=deleteHostId]),
        changeType = Table.TransformColumnTypes(deleteHostId,{{"TOTAL_SHOWS", Int64.Type}, {"TOTAL_CLICKS", Int64.Type}, {"AVG_SHOW_POSITION", type number}, {"AVG_CLICK_POSITION", type number}, {"Период", type text}, {"unicode_host_url", type text}, {"query_id", type text}, {"query_text", type text}})

    in
        R
in
    searchQueries

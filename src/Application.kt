package me.votez.ktor.example

import io.ktor.application.*
import io.ktor.features.CallLogging
import io.ktor.features.ContentNegotiation
import io.ktor.gson.gson
import io.ktor.request.path
import io.ktor.response.respond
import io.ktor.response.respondText
import io.ktor.routing.get
import io.ktor.routing.routing
import io.r2dbc.h2.H2ConnectionConfiguration
import io.r2dbc.h2.H2ConnectionFactory
import io.r2dbc.h2.H2ConnectionOption
import io.r2dbc.pool.ConnectionPool
import io.r2dbc.pool.ConnectionPoolConfiguration
import io.r2dbc.spi.Row
import io.r2dbc.spi.RowMetadata
import kotlinx.coroutines.launch
import kotlinx.coroutines.reactive.awaitFirst
import kotlinx.coroutines.reactive.awaitFirstOrNull
import kotlinx.coroutines.reactive.awaitSingle
import org.slf4j.event.Level
import reactor.core.publisher.Mono
import reactor.kotlin.core.publisher.toFlux
import kotlin.time.ExperimentalTime
import kotlin.time.seconds
import kotlin.time.toJavaDuration

fun main(args: Array<String>): Unit = io.ktor.server.netty.EngineMain.main(args)

typealias R2DBCResult = io.r2dbc.spi.Result

@ExperimentalTime
@Suppress("unused") // Referenced in application.conf
@kotlin.jvm.JvmOverloads
fun Application.module(testing: Boolean = false) {
    // Сюда запишем логику инициализации перед готовностью сервиса
    initDatabase@
    ""
    val connectionFactory = H2ConnectionFactory(
        H2ConnectionConfiguration.builder()
            .inMemory("users")
            .property(H2ConnectionOption.DB_CLOSE_DELAY, "-1")
            .build()
    )

    val poolConfig = ConnectionPoolConfiguration.builder(connectionFactory)
        .maxIdleTime(10.seconds.toJavaDuration())
        .maxSize(20)
        .build()

    val pool = ConnectionPool(poolConfig)

    // и тут закончим

    applicationStarted@
    // тут разместим служебный код для прогрева пула
    environment.monitor.subscribe(ApplicationStarted) {
        launch {
            val defer: Mono<Int> = pool.warmup()
            defer.awaitFirst()
            log.debug("Pool is hot, welcome!")
        }
    }

    boringStandardInitialization@
    // стандартный сгенерированный кусок, который не трогаем
    install(ContentNegotiation) {
        gson {
            setPrettyPrinting()
        }
    }

    install(CallLogging) {
        level = Level.INFO
        filter { call -> call.request.path().startsWith("/") }
    }

    routing {
        get("/tables") {
            showTables@
            // тут будем выводить список таблиц в базе
            ""
            val connection = pool.create().awaitSingle() // мы в корутине - ждать можно!
            val list = try {
                val result: List<R2DBCResult> = connection.createStatement(
                        """
                            select 
                                TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE, STORAGE_TYPE, SQL, ID, TYPE_NAME, TABLE_CLASS, ROW_COUNT_ESTIMATE
                            from 
                                INFORMATION_SCHEMA.TABLES
                                """.trimIndent()
                    )
                    .execute()  // тут вернётся реактивный поток
                    .toFlux()   // который мы преобразуем в удобный Reactor Flux
                    .collectList()  // который умеет собирать поток событий с данными в одно событие со списком данных
                    .awaitFirst()   // подождем, пока все соберется - мы же в корутине.
                convertData@
                result.flatMap {// один результат может породить несколько записей. Как - это дело драйвера, мы только принимаем факт
                    it
                        .map(Tables.DB::ofRow)  // преобразуем данные в поток объектов
                        .toFlux()               // который мы преобразуем в удобный Reactor Flux
                        .collectList()          // который умеет собирать поток событий с данными в одно событие со списком данных
                        .awaitFirst()          // подождем, пока все соберется - мы же в корутине.
                }
            } finally {
                connection
                    .close()    // реактивно закроем соединение
                    .awaitFirstOrNull() // и подождем - мы же в корутине.
            }

            call.respond(list)
        }

        get("/pool") {
            showPool@
            // тут будем показывать статистику пула соединений
            call.respondText {
                (pool.metrics.map {
                    """
                Max allocated size:                 ${it.maxAllocatedSize}
                Max pending size  :                 ${it.maxPendingAcquireSize}
                Acquired size     :                 ${it.acquiredSize()}
                Pending acquire size:               ${it.pendingAcquireSize()}
                Allocated size    :                 ${it.allocatedSize()}
                Idle size         :                 ${it.idleSize()}\n
            """.trimIndent()
                }.orElse("NO METRICS"))
            }
        }
    }
}

data class Tables(
    val catalog: String?,
    val schema: String?,
    val tableName: String?,
    val tableType: String?,
    val storageType: String?,
    val id: String?,
    val typeName: String?,
    val tableClass: String?,
    val rowCountEstimate: Long?
) {
    companion object DB {
        fun ofRow(r: Row, unused: RowMetadata) = Tables(
            r.get("TABLE_CATALOG", String::class.java),
            r.get("TABLE_SCHEMA", String::class.java),
            r.get("TABLE_NAME", String::class.java),
            r.get("TABLE_TYPE", String::class.java),
            r.get("STORAGE_TYPE", String::class.java),
            r.get("ID", Integer::class.java)?.toString(),
            r.get("TYPE_NAME", String::class.java),
            r.get("TABLE_CLASS", String::class.java),
            r.get("ROW_COUNT_ESTIMATE", java.lang.Long::class.java)?.toLong() ?: 0
        )
    }
}


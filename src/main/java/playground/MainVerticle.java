package playground;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.json.Json;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.openapi.RouterBuilder;
import io.vertx.ext.web.validation.RequestParameters;
import io.vertx.ext.web.validation.ValidationHandler;
import io.vertx.mysqlclient.MySQLConnectOptions;
import io.vertx.mysqlclient.MySQLPool;
import io.vertx.sqlclient.PoolOptions;
import io.vertx.sqlclient.templates.SqlTemplate;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static io.vertx.mysqlclient.MySQLPool.pool;

@SuppressWarnings("unchecked")
public class MainVerticle extends AbstractVerticle {

  public static final String GET_USER_BY_ID_OPERATION = "getUser";
  public static final String GET_ALL_USERS_OPERATION = "getUsers";
  HttpServer server;
  MySQLPool pool;

  private void initDB() {
    MySQLConnectOptions connectOptions = new MySQLConnectOptions()
      .setPort(3306)
      .setHost("localhost")
      .setDatabase("demo")
      .setUser("root")
      .setPassword("example");

    PoolOptions poolOptions = new PoolOptions()
      .setMaxSize(5);

    pool = pool(vertx, connectOptions, poolOptions);
    pool(vertx, connectOptions, poolOptions);
  }

  /**
   * prepare the result/response
   *
   * @param routingContext the routing context
   * @param code           the result code
   * @param response       the result as string
   */
  private void prepareResponse(RoutingContext routingContext, int code, String response) {
    routingContext
      .response()
      .setStatusCode(code)
      .setStatusMessage("OK")
      .end(response);
  }

  private void buildGetUserByIdRoute(RouterBuilder routerBuilder) {
    routerBuilder
      .operation(GET_USER_BY_ID_OPERATION)
      .handler(routingContext -> {
        String param = ((RequestParameters) routingContext.get(ValidationHandler.REQUEST_CONTEXT_KEY)).pathParameter("id").getInteger().toString();
        getUserById(param)
          .onComplete(userAsyncResult -> {
            String json = Optional.ofNullable(userAsyncResult.result()).map(Json::encode).orElse("{}");
            prepareResponse(routingContext, 200, json);
          })
          .onFailure(cause -> prepareResponse(routingContext, 500, cause.getMessage()));
      })
      .failureHandler(routingContext -> {
        // Handle failure
      });
  }

  private void buildGetUsersRoute(RouterBuilder routerBuilder) {
    routerBuilder
      .operation(GET_ALL_USERS_OPERATION)
      .handler(routingContext -> getAllUsers()
        .onComplete(users -> {
          String json = Optional.ofNullable(users.result()).map(Json::encode).orElse("{}");
          prepareResponse(routingContext, 200, json);
        }).onFailure(cause -> prepareResponse(routingContext, 500, cause.getMessage())));
  }

  @Override
  public void start(Promise<Void> startPromise)  {
    initDB();
    RouterBuilder.create(vertx, "src/main/resources/service-config.yaml")
      .onSuccess(routerBuilder -> {
        buildGetUserByIdRoute(routerBuilder);
        buildGetUsersRoute(routerBuilder);

        Router router = routerBuilder.createRouter();

        server = vertx.createHttpServer(new HttpServerOptions().setPort(8880).setHost("localhost"));
        server.requestHandler(router).listen();
      })
      .onFailure(err -> {
        // Handle failure
      });
  }

  public Future<User> getUserById(String userId) {
    Promise p = Promise.promise();
    String query = "SELECT * FROM users WHERE id = #{id}";

    SqlTemplate.forQuery(pool, query)
      .mapTo(User.MAP_USER)
      .execute(Collections.singletonMap("id", userId))
      .otherwiseEmpty()
      .onSuccess(users -> {
        if (users.iterator().hasNext()) {
          p.complete(users.iterator().next());
        } else {
          p.complete();
        }
      })
      .onFailure(p::fail);

    return p.future();
  }


  public Future<List<User>> getAllUsers() {
    Promise p = Promise.promise();
    String query = "SELECT * FROM users";
    SqlTemplate.forQuery(pool, query)
      .mapTo(User.MAP_USER)
      .execute(Collections.emptyMap())
      .onSuccess(users -> p.complete(User.MAP_USERS(users)))
      .onFailure(p::fail);
    return p.future();
  }

  @Override
  public void stop() {
    this.server.close();
    this.pool.close();
  }

  public static void main(String[] args) {
    Vertx vertx = Vertx.vertx();
    vertx.deployVerticle(new MainVerticle());
  }
}

package playground;

import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.templates.RowMapper;
import lombok.Builder;
import lombok.Data;

import java.util.ArrayList;
import java.util.List;

@Builder
@Data
public class User {
  private String id;
  private String name;

  public static RowMapper<User> MAP_USER = row -> User.builder()
    .id(row.getInteger("id").toString())
    .name(row.getString("name"))
    .build();

  public static List<User> MAP_USERS(RowSet<User> users) {
    List<User> result = new ArrayList<>();
    users.forEach(result::add);
    return result;
  }
}

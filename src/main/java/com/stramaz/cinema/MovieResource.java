package com.stramaz.cinema;

import io.smallrye.common.annotation.Blocking;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.pgclient.PgPool;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import javax.ws.rs.*;
import javax.ws.rs.core.Response;
import java.net.URI;

@Slf4j
@Path("movies")
public class MovieResource {

    @Inject
    PgPool client;

    @PostConstruct
//    @Blocking
    void config() {
        initdb();
    }

    @GET
    @Blocking
    public Multi<Movie> getAll() {
        log.info("get all movies");
        return Movie.findAll(client);
    }

    @GET
    @Path("{id}")
    @Blocking
    public Uni<Response> get(@PathParam("id") Long id) {
        log.info("get by id : {}", id);
        return Movie.findById(client, id)
                .onItem()
                .transform(movie -> movie != null ? Response.ok(movie) : Response.status(Response.Status.NOT_FOUND))
                .onItem()
                .transform(Response.ResponseBuilder::build);
    }

    @POST
    @Blocking
    public Uni<Response> create(Movie movie) {
        return Movie.save(client, movie.getTitle())
                .onItem()
                .transform(id -> URI.create("/movies/" + id))
                .onItem()
                .transform(uri -> Response.created(uri).build());
    }

    @DELETE
    @Path("{id}")
    @Blocking
    public Uni<Response> delete(Long id) {
        return Movie.delete(client, id)
                .onItem()
                //.transform(deleted -> deleted ? Response.status(Response.Status.NO_CONTENT) : Response.status(Response.Status.NOT_FOUND))
                // Another possible way to do it:
                .transform(deleted -> deleted ? Response.Status.NO_CONTENT : Response.Status.NOT_FOUND)
                .onItem()
                .transform(status -> Response.status(status).build());
    }

    private void initdb() {
        client.query("DROP TABLE IF EXISTS movies").execute()
                .flatMap(m -> client.query("CREATE TABLE movies (id SERIAL PRIMARY KEY, " +
                        "title TEXT NOT NULL)").execute())
                .flatMap(m -> client.query("INSERT INTO movies (title) VALUES ('The lord of the Rings')").execute())
                .flatMap(m -> client.query("INSERT INTO movies (title) VALUES ('Harry Potter')").execute())
                .await()
                .indefinitely();
    }


}

package com.doth.books_app_loader;

import com.doth.books_app_loader.author.Author;
import com.doth.books_app_loader.author.AuthorRepository;
import com.doth.books_app_loader.book.Book;
import com.doth.books_app_loader.book.BookRepository;
import com.doth.books_app_loader.connection.DataStaxAstraProperties;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.cassandra.CqlSessionBuilderCustomizer;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@SpringBootApplication
@EnableConfigurationProperties(DataStaxAstraProperties.class)
public class BooksAppLoaderApplication {

    @Autowired
    AuthorRepository authorRepository;

    @Autowired
    BookRepository bookRepository;

    @Value("${datadump.location.authors}")
    private String author;

    @Value("${datadump.location.works}")
    private String works;

    public static void main(String[] args) {
        SpringApplication.run(BooksAppLoaderApplication.class, args);
    }

    private void initAuthors(){
        Path path = Paths.get(author);
        try(Stream<String> lines = Files.lines(path)){
            lines.forEach(line ->{
                String jsonString = line.substring(line.indexOf("{"));
                JSONObject jsonObject = null;
                try {
                    jsonObject = new JSONObject(jsonString);
                    Author author = new Author();
                    author.setName(jsonObject.getString("name"));
                    author.setId(jsonObject.getString("key").replace("/authors/", ""));

                    authorRepository.save(author);
                } catch (JSONException e) {
                    e.printStackTrace();
                }

               });
        }catch (IOException e){
            e.printStackTrace();
        }
    }

    private void initWorks(){
        Path path = Paths.get(works);
        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS");
        try(Stream<String> lines = Files.lines(path)){
            lines.forEach(line ->{
                String jsonString = line.substring(line.indexOf("{"));
                JSONObject jsonObject = null;
                try {
                    jsonObject = new JSONObject(jsonString);

                    Book book = new Book();
                    book.setId(jsonObject.getString("key").replace("/works/",""));
                    book.setName(jsonObject.getString("title"));
                    JSONObject descriptionObj = jsonObject.optJSONObject("description");
                    if(descriptionObj != null){
                        book.setBookDescription(descriptionObj.getString("value"));
                    }

                    JSONObject publishedObj = jsonObject.getJSONObject("created");
                    if(publishedObj != null){
                        book.setPublishedDate(LocalDate.parse(publishedObj.getString("value"), dateTimeFormatter));
                    }

                    JSONArray coversArray = jsonObject.optJSONArray("covers");
                    if(coversArray != null){
                        List<String> coverIds = new ArrayList<>();
                        for(int i=0; i < coversArray.length(); i++){
                            coverIds.add(coversArray.getString(i));
                        }
                        book.setCoverIds(coverIds);
                    }

                    JSONArray authorsArray = jsonObject.optJSONArray("authors");
                    if(authorsArray != null){
                        List<String> authorIds = new ArrayList<>();
                        for(int i=0; i < authorsArray.length(); i++){
                            String authorId = authorsArray.getJSONObject(i).getJSONObject("author")
                                    .getString("key").replace("/authors/", "");
                            authorIds.add(authorId);
                        }
                        book.setAuthorIds(authorIds);
                        List<String> authorNames = authorIds.stream()
                                .map(id -> authorRepository.findById(id))
                                .map(optionalAuthor -> {
                                    if (!optionalAuthor.isPresent()) return "Unknown Author";
                                    return optionalAuthor.get().getName();
                                }).collect(Collectors.toList());
                        book.setAuthorNames(authorNames);
                    }


                    bookRepository.save(book);
                } catch (JSONException e) {
                    e.printStackTrace();
                }

            });
        }catch (IOException e){
            e.printStackTrace();
        }
    }

    @PostConstruct
    public void start(){
        initAuthors();
        initWorks();
    }


    @Bean
    public CqlSessionBuilderCustomizer sessionBuilderCustomizer(DataStaxAstraProperties astraProperties){
        Path bundle = astraProperties.getSecureConnectBundle().toPath();
        return builder -> builder.withCloudSecureConnectBundle(bundle);
    }
}

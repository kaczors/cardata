package com.github.pawel.cardata;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;
import static org.springframework.util.StringUtils.isEmpty;

@Component
class CardataApplicationRunner implements CommandLineRunner {
    private static final ObjectMapper objectMapper = new ObjectMapper();
    public static final String ROOT_URL = "https://www.autocentrum.pl";
    private static final String CSS_QUERY_WIDTH = "div:matchesOwn(^Szerokość$) + div > span.dt-param-edit + span.dt-param-value";
    private static final String CSS_QUERY_LENGTH = "div:matchesOwn(^Długość$) + div > span.dt-param-edit + span.dt-param-value";
    private static final String CSS_QUERY_HEIGHT = "div:matchesOwn(^Wysokość$) + div > span.dt-param-edit + span.dt-param-value";
    private static final String CSS_QUERY_SEATS = "div:containsOwn(Liczba miejsc) + div > span.dt-param-edit + span.dt-param-value";
    private static final String CSS_QUERY_DOORS = "div:containsOwn(Liczba drzwi) + div > span.dt-param-edit + span.dt-param-value";
    private static final String CSS_QUERY_TRUNK = "div:containsOwn(Minimalna pojemność bagażnika) + div > span.dt-param-edit + span.dt-param-value";
    private static final String CSS_QUERY_YEAR = "div:matchesOwn(^Produkowany$) + div > span.dt-param-edit + span.dt-param-value";
    private static final String CSS_QUERY_CC = "div:matchesOwn(^Pojemność skokowa$) + div > span.dt-param-edit + span.dt-param-value";
    private static final String CSS_QUERY_FUEL = "div:matchesOwn(^Typ silnika$) + div > span.dt-param-value";
    private static final String CSS_QUERY_HP = "div:matchesOwn(^Moc silnika$) + div > span.dt-param-edit + span.dt-param-value";
    private static final String CSS_QUERY_V_MAX = "div:matchesOwn(^Prędkość maksymalna$) + div > span.dt-param-edit + span.dt-param-value";
    private static final String CSS_QUERY_ACC = "div:matchesOwn(^Przyspieszenie \\(od 0 do 100km/h\\)$) + div > span.dt-param-edit + span.dt-param-value";
    private static final String CSS_QUERY_WEIGHT = "div:containsOwn(Minimalna masa własna pojazdu) + div > span.dt-param-edit + span.dt-param-value";
    private static final String CSS_QUERY_FUEL_REPORTS_COUNT = "div:containsOwn(Liczba raportów:) + div.sateh-right > div.satehr-small";
    private static final String CSS_QUERY_AVG_FUEL = "div:containsOwn(Średnia z powyższych) + div.sateh-right > div.satehr-small";
    private static final String CSS_QUERY_RATING_COUNT = "h2:contains(ocen tego auta)";
    private static final String CSS_QUERY_RATING = "div:containsOwn(Średnia ocena) + span > div.badge";

    @Override
    public void run(String... strings) throws Exception {

        PrintWriter printWriter = new PrintWriter(new FileOutputStream("c:/development/other/data.json"));

        getBrandUrls()
                .parallel()
                .flatMap(this::getModelUrls)
                .flatMap(this::getGeneration)
                .flatMap(this::getVersion)
                .flatMap(this::getEngineUrls)
                .peek(System.out::println)
                .map(this::getDetails)
                .forEach(carDetails -> printWriter.println(toJson(carDetails)));

        printWriter.flush();
    }

    private Stream<String> getBrandUrls() throws IOException {
        return Jsoup.connect(ROOT_URL + "/dane-techniczne/")
                .get()
                .select("div.mark-list a")
                .stream()
                .map(e -> e.attr("href"));
    }

    private Stream<String> getModelUrls(String brandUrl) {
        return getDocument(ROOT_URL + brandUrl)
                .select("div.car-selector-box-row a")
                .stream()
                .map(e -> e.attr("href"));
    }

    private Stream<String> getGeneration(String modelUrl) {
        List<String> versions = getDocument(ROOT_URL + modelUrl)
                .select("div.car-selector-box-row a")
                .stream()
                .map(e -> e.attr("href"))
                .collect(toList());
        return versions.isEmpty() ? Stream.of(modelUrl) : versions.stream();
    }

    private Stream<String> getVersion(String modelUrl) {
        List<String> versions = getDocument(ROOT_URL + modelUrl)
                .select("div.car-selector-box-row a")
                .stream()
                .map(e -> e.attr("href"))
                .collect(toList());
        return versions.isEmpty() ? Stream.of(modelUrl) : versions.stream();
    }

    private Stream<String> getEngineUrls(String modelUrl) {
        return getDocument(ROOT_URL + modelUrl)
                .select("div.engine-box a")
                .stream()
                .map(e -> e.attr("href"));
    }

    private Map<String, Object> getDetails(String modelAndEngineUrl) {

        Map<String, Object> parameters = new HashMap<>();
        Document document = getDocument(ROOT_URL + modelAndEngineUrl);

        parameters.put("url", ROOT_URL + modelAndEngineUrl);
        parameters.put("name", getName(document));
        parameters.put("doors", getDoors(document));
        parameters.put("seats", getSeats(document));
        parameters.put("length", getLength(document));
        parameters.put("width", getWidth(document));
        parameters.put("height", getHeight(document));
        parameters.put("trunk", getTrunk(document));
        parameters.put("year-from", getYearFrom(document));
        parameters.put("year-to", getYearTo(document));
        parameters.put("cc", getCc(document));
        parameters.put("fuel", getFuel(document));
        parameters.put("hp", getHp(document));
        parameters.put("vmax", getVmax(document));
        parameters.put("acc", getAcc(document));
        parameters.put("weight", getWeight(document));

        String fuelUrl = modelAndEngineUrl.replaceAll("dane-techniczne", "spalanie");
        Document fuelDocument = getDocument(ROOT_URL + fuelUrl);

        parameters.put("fuel-reports-count", getFuelReportsCount(fuelDocument));
        parameters.put("fuel-avg", getFuelAvg(fuelDocument));
        parameters.put("lpg-fuel-avg", getLpgFuelAvg(fuelDocument));
        parameters.put("lpg-reports-count", getLpgFuelReportsCount(fuelDocument));

        String ratingUrl = fuelUrl.replaceAll("spalanie", "oceny");
        Document ratingDocument = getDocument(ROOT_URL + ratingUrl);

        parameters.put("rating-count", getRatingCount(ratingDocument));
        parameters.put("rating", getRating(ratingDocument));

        return parameters;
    }

    private Double getRating(Document document) {
        return selectDouble(document, CSS_QUERY_RATING, s -> s.replaceAll(",", ".").trim());
    }

    private Integer getRatingCount(Document document) {
        return selectInteger(document, CSS_QUERY_RATING_COUNT, s -> s.split(" ")[1]);
    }

    private Double getFuelAvg(Document document) {
        return selectDouble(document, CSS_QUERY_AVG_FUEL, s -> s.replaceAll(",", ".").replaceAll("-", "").trim());
    }

    private Double getLpgFuelAvg(Document document) {
        Elements elements = document
                .select(CSS_QUERY_AVG_FUEL);

        Element element = elements.size() > 1 ? elements.get(1) : null;

        String text = element == null ? "" : element.text();

        String transformed = isEmpty(text) ? text : text.replaceAll(",", ".").replaceAll("-", "").trim();
        return isEmpty(transformed) ? null : Double.valueOf(transformed);
    }

    private Integer getFuelReportsCount(Document document) {
        return selectInteger(document, CSS_QUERY_FUEL_REPORTS_COUNT, String::trim);
    }

    private Integer getLpgFuelReportsCount(Document document) {
        Elements elements = document
                .select(CSS_QUERY_FUEL_REPORTS_COUNT);

        Element element = elements.size() > 1 ? elements.get(1) : null;


        String text = element == null ? "" : element.text();

        String transformed = isEmpty(text) ? text : text.trim();
        return isEmpty(transformed) ? null : Integer.valueOf(transformed);
    }

    private Integer getWeight(Document document) {
        return selectInteger(document, CSS_QUERY_WEIGHT, s -> s.replaceAll("kg", "").trim());
    }

    private Double getAcc(Document document) {
        return selectDouble(
                document,
                CSS_QUERY_ACC,
                s -> s.replaceAll("s", "").replaceAll(",", ".").trim());
    }

    private String getName(Document document) {
        return document
                .select("h1.site-title")
                .text()
                .replaceAll("Dane techniczne", "")
                .trim();
    }

    private Integer getDoors(Document document) {
        return selectInteger(document, CSS_QUERY_DOORS);
    }

    private Integer getSeats(Document document) {
        return selectInteger(document, CSS_QUERY_SEATS);
    }

    private Integer getLength(Document document) {
        return selectInteger(document, CSS_QUERY_LENGTH, s -> s.replaceAll("mm", "").trim());
    }

    private Integer getWidth(Document document) {
        return selectInteger(document, CSS_QUERY_WIDTH, s -> s.replaceAll("mm", "").trim());
    }

    private Integer getHeight(Document document) {
        return selectInteger(document, CSS_QUERY_HEIGHT, s -> s.replaceAll("mm", "").trim());
    }

    private Integer getTrunk(Document document) {
        return selectInteger(document, CSS_QUERY_TRUNK, s -> s.replaceAll("l", "").trim());
    }

    private Integer getCc(Document document) {
        return selectInteger(document, CSS_QUERY_CC, s -> s.replaceAll("cm3", "").trim());
    }

    private Integer getYearFrom(Document document) {
        return selectInteger(document, CSS_QUERY_YEAR, s -> s.trim().split(" ")[1]);
    }

    private Integer getYearTo(Document document) {
        return selectInteger(document, CSS_QUERY_YEAR, s -> {
            String[] split = s.trim().split(" ");
            return split.length == 5 ? split[3] : null;
        });
    }

    private String getFuel(Document document) {
        return document.select(CSS_QUERY_FUEL).text().trim();
    }

    private String getLpgFuel(Document document) {
        return document.select(CSS_QUERY_FUEL).text().trim();
    }

    private Integer getHp(Document document) {
        return selectInteger(document, CSS_QUERY_HP, s -> s.trim().split(" ")[0]);
    }

    private Integer getVmax(Document document) {
        return selectInteger(document, CSS_QUERY_V_MAX, s -> s.replaceAll("km/h", "").trim());
    }

    private Integer selectInteger(Document document, String cssQuery) {
        return selectInteger(document, cssQuery, Function.identity());
    }

    private Integer selectInteger(Document document, String cssQuery, Function<String, String> transformations) {
        return select(document, cssQuery, transformations, Integer::valueOf);
    }

    private Double selectDouble(Document document, String cssQuery, Function<String, String> transformations) {
        return select(document, cssQuery, transformations, Double::valueOf);
    }

    private <T> T select(Document document, String cssQuery, Function<String, String> transformations, Function<String, T> factory) {
        Element element = document
                .select(cssQuery)
                .first();

        String text = element == null ? "" : element.text();

        String transformed = isEmpty(text) ? text : transformations.apply(text);
        return isEmpty(transformed) ? null : factory.apply(transformed);
    }

    private Document getDocument(String url) {
        try {
            return Jsoup.connect(url).get();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private String toJson(Object object) {
        try {
            return objectMapper.writeValueAsString(object);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}

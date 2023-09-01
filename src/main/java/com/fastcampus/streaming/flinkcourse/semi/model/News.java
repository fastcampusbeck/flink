package com.fastcampus.streaming.flinkcourse.semi.model;

import java.time.LocalDateTime;
import java.util.Objects;

public class News {
    private String title;
    private String content;
    private long broadcast_date;

    public News() {
    }

    public News(String title, String content, long broadcast_date) {
        this.title = title;
        this.content = content;
        this.broadcast_date = broadcast_date;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    public long getBroadcast_date() {
        return broadcast_date;
    }

    public void setBroadcast_date(long broadcast_date) {
        this.broadcast_date = broadcast_date;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        News news = (News) o;
        return broadcast_date == news.broadcast_date && Objects.equals(title, news.title) && Objects.equals(content, news.content);
    }

    @Override
    public int hashCode() {
        return Objects.hash(title, content, broadcast_date);
    }

    @Override
    public String toString() {
        return "News{" +
                "title='" + title + '\'' +
                ", content='" + content + '\'' +
                ", broadcast_date=" + broadcast_date +
                '}';
    }
}

package ir.milux.metalmarks.server;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;

import com.google.common.cache.*;
import ir.milux.metalmarks.core.CONSTANTS;
import ir.milux.metalmarks.core.DB;
import ir.milux.metalmarks.core.MOB;
import org.apache.log4j.Logger;
import org.eclipse.jetty.http.HttpStatus;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.thread.QueuedThreadPool;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;


public class RestServer extends HttpServlet {
    private static Logger logger = Logger.getRootLogger();

    public static void main(String[] args) throws Exception {
        QueuedThreadPool threadPool = new QueuedThreadPool(400, 20);
        Server server = new Server(threadPool);
        ServerConnector http = new ServerConnector(server);
        http.setPort(CONSTANTS.REST_PORT);
        server.addConnector(http);
        ServletContextHandler handler = new ServletContextHandler(server, "/");
        Byte placeHolder = 1;
        LoadingCache<String,Byte> cachedKeys = CacheBuilder.newBuilder()
                .maximumSize(50000)
                .expireAfterAccess(2000, TimeUnit.HOURS)
                .removalListener(new RemovalListener<String,Byte>() {
                    @Override
                    public void onRemoval(RemovalNotification<String,Byte> removalNotification) {
                        if (removalNotification.getCause() == RemovalCause.REPLACED) {
                            return;
                        }

                        if (removalNotification.getCause() == RemovalCause.EXPIRED) {
                            logger.info("key : " + removalNotification.getKey() + " removed from cache !");
                            String cachedFile = CONSTANTS.CCH_BASE + removalNotification.getKey().replace("/", "_");
                            try {
                                Files.deleteIfExists(Paths.get(cachedFile));
                            } catch (IOException e) {
                                logger.warn("can not remove file : " + e.getMessage());
                            }

                        }
                    }
                })
                .build(new CacheLoader<String,Byte>() {
                    @Override
                    public Byte load(String key) throws Exception {
                        return placeHolder;
                    }
                });

        handler.addServlet(new ServletHolder(new HttpServlet() {
            @Override
            protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
                resp.setStatus(HttpStatus.OK_200);
                String originalFileName = req.getParameter("query");
                String cachedFileName = CONSTANTS.CCH_BASE + originalFileName.replace("/", "_");
                Path cachedFilePath = Paths.get(cachedFileName);
                resp.setHeader("Content-Type", Files.probeContentType(cachedFilePath));

                if (cachedKeys.getIfPresent(originalFileName) != null)
                    try {
                        resp.getOutputStream().write(Files.readAllBytes(cachedFilePath));
                        logger.info("cache hit : " + originalFileName);
                        cachedKeys.refresh(originalFileName);
                        return;
                    } catch (FileNotFoundException e) {
                        logger.info("file not found in file system : " + originalFileName);
                    } catch (NoSuchFileException e) {
                        logger.info("file not found in file system : " + originalFileName);
                    }

                MOB mob = DB.get(originalFileName);

                if (mob == null) {
                    resp.setStatus(HttpStatus.NOT_FOUND_404);
                    resp.getWriter().write("<html><h1><center> Not Found (: </h1></center</br><p><center>" +
                            "powered by metalmark</center></p></html>");
                    logger.warn("File Not Found : " + originalFileName);
                    return;
                }

                try {
                    FileOutputStream outputStream = new FileOutputStream(cachedFileName);
                    outputStream.write(mob.getBinary());
                    outputStream.flush();
                    outputStream.close();
                    cachedKeys.refresh(originalFileName);
                    resp.setHeader("Content-Type", mob.getTag());
                    resp.getOutputStream().write(mob.getBinary());
                } catch (Exception e) {
                    logger.info(e.getMessage());
                }
                return;
            }
        }), "/mob");

        server.start();

    }
}
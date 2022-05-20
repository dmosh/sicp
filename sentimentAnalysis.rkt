#lang racket
(require data-science)
(require plot)
(require math)
(require json)
(require srfi/19)
(require racket/stream)


;;; This function reads line-oriented JSON (as output by massmine),and packages it into an array. For very large data sets, loading
;;; everything into memory like this is heavy handed. For data this small,working in memory is simpler

(define (json-lines->json-array #:head [head #f])
  (let loop ([num 0]
             [json-array '()]
             [record (read-json (current-input-port))])
    (if (or (eof-object? record)
            (and head (>= num head)))
        (jsexpr->string json-array)
        (loop (add1 num) (cons record json-array)
              (read-json (current-input-port))))))

;;; Normalize case, remove URLs, remove punctuation, and remove spaces
;;; from each tweet. This function takes a list of words and returns a
;;; preprocessed subset of words/tokens as a list

(define (preprocess-text lst)
  (map (λ (x)
         (string-normalize-spaces
          (remove-punctuation
           (remove-urls
            (string-downcase x))) #:websafe? #t))
       lst))

;;; Read in the entire tweet dataset Timeline  (3242) TWEETS

(define tweets (string->jsexpr
                (with-input-from-file "Racketsample1.json" (λ () (json-lines->json-array)))))

;;t is a list of lists of strings. Tail recursion is used to extract each string and append
;; it into one large string.

(define list-string
  (let ([tmp (map (λ (x) (list (hash-ref x 'text)(hash-ref x 'created_at) (hash-ref x 'source))) tweets)]) ;; improve to use streams
    (filter (λ (x) (not (string-prefix? (first x) "RT"))) tmp)
    ))

; Joining tweets and arranging tweets to their systematic flow in and out.

(define joined-tweets
    (local[
           (define (joined1 tlist1 acc)
             (cond [(empty? tlist1) acc]
                   [else (joined1 (rest tlist1) (string-join (list acc "\n " (first(first tlist1)))))]
                   )
             )
           ](joined1 list-string "")) )

;;; To begin our sentiment analysis, we extract each unique word
;;; and the number of times it occurred in the document
(define words (document->tokens joined-tweets #:sort? #t))


(define android (filter (λ (x) (string=? (second x) "Twitter for Android")) list-string))
(define iphone (filter (λ (x) (string=? (second x) "Twitter for iPhone")) list-string))
(define web (filter (λ (x) (string=? (second x) "Twitter Web App")) list-string))

;;; Using the nrc lexicon, we can label each (non stop-word) with an
;;; emotional label. 
(define sentiment (list->sentiment words #:lexicon 'nrc))

(take sentiment 5)

;;; sentiment, created above, consists of a list of triplets of the pattern
;;; (token sentiment freq) for each token in the document. Many words will have 
;;; the same sentiment label, so we aggregrate (by summing) across such tokens.
(aggregate sum ($ sentiment 'sentiment) ($ sentiment 'freq))


;;; Better yet, we can visualize this result as a barplot (discrete-histogram)

;;(remove-stopwords words)
(define cleaned-texts (remove-punctuation (remove-urls joined-tweets)))
(define word-counts (document->tokens cleaned-texts #:sort? #t))
(define clean-word-counts (remove-stopwords word-counts))
(define moods (list->sentiment clean-word-counts #:lexicon 'nrc))
(take moods 5)

(aggregate sum ($ moods 'sentiment) ($ moods 'freq))

(let ([counts (aggregate sum ($ moods 'sentiment) ($ moods 'freq))])
  (parameterize ((plot-width 800))
    (plot (list
	   (tick-grid)
	   (discrete-histogram
	    (sort counts (λ (x y) (> (second x) (second y))))
	    #:color "green"
	    #:line-color "MediumSlateBlue"))
	  #:x-label "Affective Label"
	  #:y-label "Frequency")))

(define moods2 (list->sentiment clean-word-counts #:lexicon 'bing))
(parameterize ([plot-height 200])
  (plot (discrete-histogram
	 (aggregate sum ($ moods2 'sentiment) ($ moods2 'freq))
	 #:y-min 0
	 #:y-max 8000
	 #:invert? #t
	 #:color "green"
	 #:line-color "MediumOrchid")
	#:x-label "Frequency"
	#:y-label "Sentiment Polarity"))

(let ([counts (aggregate sum ($ moods2 'sentiment) ($ moods2 'freq))])
  (chi-square-goodness counts '(.5 .5)))

;;; We can also look at which words are contributing the most to our
;;; positive and negative sentiment scores.
(define negatives
  (take (cdr (subset moods2 'sentiment "negative")) 10))
(define positives
  (take (cdr (subset moods2 'sentiment "positive")) 10))

;;; Some clever reshaping for plotting purposes
(define n (map (λ (x) (list (first x) (- 0 (third x))))
	       negatives))
(define p (sort (map (λ (x) (list (first x) (third x)))
		     positives)
		(λ (x y) (< (second x) (second y)))))

;;; Plot the results
(parameterize ((plot-width 800)
	       (plot-x-tick-label-anchor 'right)
	       (plot-x-tick-label-angle 90))
  (plot (list
	 (tick-grid)
	 (discrete-histogram n #:y-min -120
			     #:y-max 655
			     #:color "blue"
			     #:line-color "white"
			     #:label "Negative Sentiment") 
	 (discrete-histogram p #:y-min -116
			     #:y-max 649
			     #:x-min 15
			     #:color "green"
			     #:line-color "LightSeaGreen"
			     #:label "Positive Sentiment"))
	#:x-label "Word"
	#:y-label "Contribution to sentiment"))
	
;;; Plot the top 20 words from both devices combined
(parameterize ([plot-width 600]
               [plot-height 600])
    (plot (list
        (tick-grid)
        (discrete-histogram (reverse (take clean-word-counts 50))
                            #:invert? #t
                            #:color "green"
                            #:line-color "white"
                            #:y-max 450))
       #:x-label "Occurances"
       #:y-label "Words"))

;;;time analysis
(string->date "11/10/2020" "~m/~d/~Y")

(display (string->date "2021 06 05T01:58:13 000Z" "~Y ~m ~dT~H:~M:~S 000Z"))

(define (convert-timestamp str)
  (string->date str "~Y ~m ~dT~H:~M:~S 000Z"))

;;; Tweets with Timestamps 


(define timestamp-by-type
  (map (λ (x) (list (second x) (first x)
                    ))
       list-string))

(define (bin-timestamps timestamps)
  (let ([time-format "~H"])
    ;; Return
    (sorted-counts
     (map (λ (x) (date->string (convert-timestamp x) time-format)) timestamps))))



(define a-time
  (bin-timestamps ($ (subset timestamp-by-type 1 (λ (x) (string=? x ))) 0)))

(define a-time2 (map (λ (x) (list (string->number (first x)) (second x))) a-time))

(define (fill-missing-bins lst bins)
  (define (get-count lst val)
    (let ([count (filter (λ (x) (equal? (first x) val)) lst)])
      (if (null? count) (list val 0) (first count))))
  (map (λ (bin) (get-count lst bin))
       bins))

;;; Convert UTC time to EAT
(define (time-EAT lst)
  (map list
       ($ lst 0)
       (append (drop ($ lst 1) 3) (take ($ lst 1) 3))))

;;; Convert bin counts to percentages
(define (count->percent lst)
  (let ([n (sum ($ lst 1))])
    (map list
         ($ lst 0)
         (map (λ (x) (* 100 (/ x n))) ($ lst 1)))))

(let ([a-data (count->percent (time-EAT (fill-missing-bins a-time2 (range 24))))]
     )
  (parameterize ([plot-legend-anchor 'top-right]
                 [plot-width 800])
      (plot (list
             (tick-grid)
             (lines a-data
                    #:color "green"
                    #:width 2
                    #:label "TWEETING TIME")
             )
            #:x-label "Hour of day (EAT)"
            #:y-label "% of tweets")))

(define (bin-month months)
  (let ([time-format "~m"])
    ;; Return
    (sorted-counts
     (map (λ (x) (date->string (convert-timestamp x) time-format)) months))))

(define tweetbymonth
  (bin-month ($ (subset timestamp-by-type 1 (λ (x) (string=? x ))) 0)))

(define tweetbymonth2 (map (λ (x) (list (string->number (first x)) (second x))) tweetbymonth))

(let ([month-data (count->percent (fill-missing-bins tweetbymonth2 (range 12)))]
     )
  (parameterize ([plot-legend-anchor 'top-right]
                 [plot-width 800])
      (plot (list
             (tick-grid)
             (lines month-data
                    #:color "green"
                    #:width 2
                    #:label "TWEETING month")
             )
            #:x-label "Month of year "
            #:y-label "% of tweets")))


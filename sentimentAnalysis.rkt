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


(define android (filter (λ (x) (string=? (second x) "android")) list-string))
(define iphone (filter (λ (x) (string=? (second x) "iphone")) list-string))

;;; Using the nrc lexicon, we can label each (non stop-word) with an
;;; emotional label. 
(define sentiment (list->sentiment words #:lexicon 'nrc))

(take sentiment 5)

;;; sentiment, created above, consists of a list of triplets of the pattern
;;; (token sentiment freq) for each token in the document. Many words will have 
;;; the same sentiment label, so we aggregrate (by summing) across such tokens.
(aggregate sum ($ sentiment 'sentiment) ($ sentiment 'freq))


;;; Better yet, we can visualize this result as a barplot (discrete-histogram)
; 


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
;;; positive and negative sentiment scores. We'll look at the top 15
;;; influential (i.e., most frequent) positive and negative words
(define negatives
  (take (cdr (subset moods2 'sentiment "negative")) 10))
(define positive-tokens
  (take (cdr (subset moods2 'sentiment "positive")) 10))

;;; Some clever reshaping for plotting purposes
(define n (map (λ (x) (list (first x) (- 0 (third x))))
	       negatives))
(define p (sort (map (λ (x) (list (first x) (third x)))
		     positive-tokens)
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

;;; Convert UTC time to EST
(define (time-EST lst)
  (map list
       ($ lst 0)
       (append (drop ($ lst 1) 3) (take ($ lst 1) 3))))

;;; Convert bin counts to percentages
(define (count->percent lst)
  (let ([n (sum ($ lst 1))])
    (map list
         ($ lst 0)
         (map (λ (x) (* 100 (/ x n))) ($ lst 1)))))

(let ([a-data (count->percent (time-EST (fill-missing-bins a-time2 (range 24))))]
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

;;; Plot number of quoted vs unquoted tweets per device
(plot (list (discrete-histogram tweetbymonth
                                #:label "Number of tweets per month"
                                #:skip 2.5
                                #:x-min 0
                                #:color "OrangeRed"
                                #:line-color "OrangeRed")
            )
      #:y-max 1000
      #:x-label "month of the year"
      #:y-label "Number of tweets")

(define (bin-month2 months)
  (let ([time-format "~m"])
    ;; Return
   (date->string (convert-timestamp months) time-format)
     ))
(define get-data-by-month
  (map (λ (x) (list (second x)
                    (cond [(string-contains? (list-ref (string-split (bin-month2 (first x)) " ") 0) "1") "1"]
                          [(string-contains? (list-ref (string-split (bin-month2 (first x)) " ") 0) "2") "2"]
                           [(string-contains? (list-ref (string-split (bin-month2 (first x)) " ") 0) "3") "3"]
                            [(string-contains? (list-ref (string-split (bin-month2 (first x)) " ") 0) "4") "4"]
                            [(string-contains? (list-ref (string-split (bin-month2 (first x)) " ") 0) "5") "5"]
                           [(string-contains? (list-ref (string-split (bin-month2 (first x)) " ") 0) "6") "6"]
                            [(string-contains? (list-ref (string-split (bin-month2 (first x)) " ") 0) "7") "7"]
                            [(string-contains? (list-ref (string-split (bin-month2 (first x)) " ") 0) "8") "8"]
                            [(string-contains? (list-ref (string-split (bin-month2 (first x)) " ") 0) "9") "9"]
                            [(string-contains? (list-ref (string-split (bin-month2 (first x)) " ") 0) "10") "10"]
                           [(string-contains? (list-ref (string-split (bin-month2 (first x)) " ") 0) "11") "11"]
                            [(string-contains? (list-ref (string-split (bin-month2 (first x)) " ") 0) "12") "12"]
                          [else "other"])))
       timestamp-by-type))

;;separate tweeets by month
(define jan (filter (λ (x) (string=? (second x) "1")) get-data-by-month))
(define feb (filter (λ (x) (string=? (second x) "2")) get-data-by-month))
(define march (filter (λ (x) (string=? (second x) "3")) get-data-by-month))
(define april (filter (λ (x) (string=? (second x) "4")) get-data-by-month))
(define may (filter (λ (x) (string=? (second x) "5")) get-data-by-month))
(define june (filter (λ (x) (string=? (second x) "6")) get-data-by-month))
(define july (filter (λ (x) (string=? (second x) "7")) get-data-by-month))
(define august (filter (λ (x) (string=? (second x) "8")) get-data-by-month))
(define september (filter (λ (x) (string=? (second x) "9")) get-data-by-month))
(define october (filter (λ (x) (string=? (second x) "10")) get-data-by-month))
(define november (filter (λ (x) (string=? (second x) "11")) get-data-by-month))
(define december (filter (λ (x) (string=? (second x) "12")) get-data-by-month))

(define jan-tweets
    (local[
           (define (joined1 tlist1 acc)
             (cond [(empty? tlist1) acc]
                   [else (joined1 (rest tlist1) (string-join (list acc "\n " (first(first tlist1)))))]
                   )
             )
           ](joined1 jan "")) )
(define feb-tweets
    (local[
           (define (joined1 tlist1 acc)
             (cond [(empty? tlist1) acc]
                   [else (joined1 (rest tlist1) (string-join (list acc "\n " (first(first tlist1)))))]
                   )
             )
           ](joined1 feb "")) )
(define march-tweets
    (local[
           (define (joined1 tlist1 acc)
             (cond [(empty? tlist1) acc]
                   [else (joined1 (rest tlist1) (string-join (list acc "\n " (first(first tlist1)))))]
                   )
             )
           ](joined1 march "")) )
(define april-tweets
    (local[
           (define (joined1 tlist1 acc)
             (cond [(empty? tlist1) acc]
                   [else (joined1 (rest tlist1) (string-join (list acc "\n " (first(first tlist1)))))]
                   )
             )
           ](joined1 april "")) )
(define may-tweets
    (local[
           (define (joined1 tlist1 acc)
             (cond [(empty? tlist1) acc]
                   [else (joined1 (rest tlist1) (string-join (list acc "\n " (first(first tlist1)))))]
                   )
             )
           ](joined1 may "")) )
(define june-tweets
    (local[
           (define (joined1 tlist1 acc)
             (cond [(empty? tlist1) acc]
                   [else (joined1 (rest tlist1) (string-join (list acc "\n " (first(first tlist1)))))]
                   )
             )
           ](joined1 june "")) )
(define july-tweets
    (local[
           (define (joined1 tlist1 acc)
             (cond [(empty? tlist1) acc]
                   [else (joined1 (rest tlist1) (string-join (list acc "\n " (first(first tlist1)))))]
                   )
             )
           ](joined1 july "")) )
(define august-tweets
    (local[
           (define (joined1 tlist1 acc)
             (cond [(empty? tlist1) acc]
                   [else (joined1 (rest tlist1) (string-join (list acc "\n " (first(first tlist1)))))]
                   )
             )
           ](joined1 august "")) )
(define september-tweets
    (local[
           (define (joined1 tlist1 acc)
             (cond [(empty? tlist1) acc]
                   [else (joined1 (rest tlist1) (string-join (list acc "\n " (first(first tlist1)))))]
                   )
             )
           ](joined1 september "")) )
(define october-tweets
    (local[
           (define (joined1 tlist1 acc)
             (cond [(empty? tlist1) acc]
                   [else (joined1 (rest tlist1) (string-join (list acc "\n " (first(first tlist1)))))]
                   )
             )
           ](joined1 october "")) )

(define november-tweets
    (local[
           (define (joined1 tlist1 acc)
             (cond [(empty? tlist1) acc]
                   [else (joined1 (rest tlist1) (string-join (list acc "\n " (first(first tlist1)))))]
                   )
             )
           ](joined1 november "")) )
(define december-tweets
    (local[
           (define (joined1 tlist1 acc)
             (cond [(empty? tlist1) acc]
                   [else (joined1 (rest tlist1) (string-join (list acc "\n " (first(first tlist1)))))]
                   )
             )
           ](joined1 december "")) )

(define jan-words (document->tokens  jan-tweets #:sort? #t))
(define feb-words (document->tokens  feb-tweets #:sort? #t))
(define march-words (document->tokens march-tweets #:sort? #t))
(define april-words (document->tokens april-tweets #:sort? #t))
(define may-words (document->tokens may-tweets #:sort? #t))
(define june-words (document->tokens june-tweets #:sort? #t))
(define july-words (document->tokens july-tweets #:sort? #t))
(define august-words (document->tokens august-tweets #:sort? #t))
(define september-words (document->tokens september-tweets #:sort? #t))
(define october-words (document->tokens october-tweets #:sort? #t))
(define november-words (document->tokens november-tweets #:sort? #t))
(define december-words (document->tokens december-tweets #:sort? #t))

(define jan-sentiment (list->sentiment (remove-stopwords jan-words) #:lexicon 'bing))
(define feb-sentiment (list->sentiment (remove-stopwords feb-words) #:lexicon 'bing))
(define march-sentiment (list->sentiment (remove-stopwords march-words) #:lexicon 'bing))
(define april-sentiment (list->sentiment (remove-stopwords april-words) #:lexicon 'bing))
(define june-sentiment (list->sentiment (remove-stopwords june-words) #:lexicon 'bing))
(define july-sentiment (list->sentiment (remove-stopwords july-words) #:lexicon 'bing))
(define august-sentiment (list->sentiment (remove-stopwords august-words) #:lexicon 'bing))
(define september-sentiment (list->sentiment (remove-stopwords september-words) #:lexicon 'bing))
(define  october-sentiment (list->sentiment (remove-stopwords october-words) #:lexicon 'bing))
(define november-sentiment (list->sentiment (remove-stopwords november-words) #:lexicon 'bing))
(define december-sentiment (list->sentiment (remove-stopwords december-words) #:lexicon 'bing))
(define may-sentiment (list->sentiment (remove-stopwords may-words) #:lexicon 'bing))

(define jan-negative-tokens
  (take (cdr (subset jan-sentiment 'sentiment "negative")) 10))
(define jan-positive-tokens
  (take (cdr (subset jan-sentiment 'sentiment "positive")) 10))


(define may-negative-tokens
  (take (cdr (subset may-sentiment 'sentiment "negative")) 10))
(define may-positive-tokens
  (take (cdr (subset may-sentiment 'sentiment "positive")) 10))
(define june-negative-tokens
  (take (cdr (subset june-sentiment 'sentiment "negative")) 10))
(define june-positive-tokens
  (take (cdr (subset june-sentiment 'sentiment "positive")) 10))
(define july-negative-tokens
  (take (cdr (subset july-sentiment 'sentiment "negative")) 10))
(define july-positive-tokens
  (take (cdr (subset july-sentiment 'sentiment "positive")) 10))
(define august-negative-tokens
  (take (cdr (subset august-sentiment 'sentiment "negative")) 10))
(define august-positive-tokens
  (take (cdr (subset august-sentiment 'sentiment "positive")) 10))
(define september-negative-tokens
  (take (cdr (subset september-sentiment 'sentiment "negative")) 10))
(define september-positive-tokens
  (take (cdr (subset september-sentiment 'sentiment "positive")) 10))


;;; Some clever reshaping for plotting purposes
(define n2 (map (λ (x) (list (first x) (- 0 (third x))))
	       jan-negative-tokens))
(define p2 (sort (map (λ (x) (list (first x) (third x)))
		     positive-tokens)
		(λ (x y) (< (second x) (second y)))))
;;; Restructure the data for our histogram below
(define positive
  (list '( "Jan" ,(second (first jan-negative-tokens)))
        '( "Feb" ,(second (first android-quotes)))
        '( "March" ,(second (first android-quotes)))
        '( "April" ,(second (first android-quotes)))
        '( "May" ,(second (first android-quotes)))
        '( "June" ,(second (first android-quotes)))
        '( "July" ,(second (first android-quotes)))
        '( "Aug" ,(second (first android-quotes)))
        '( "Sep" ,(second (first android-quotes)))
        '( "Oct" ,(second (first android-quotes)))
        '( "Nov" ,(second (first android-quotes)))
        '("Dec" ,(second (second iphone-quotes)))))
(define negative
  (list '( "Jan" ,(second (first android-quotes)))
        '( "Feb" ,(second (first android-quotes)))
        '( "March" ,(second (first android-quotes)))
        '( "April" ,(second (first android-quotes)))
        '( "May" ,(second (first android-quotes)))
        '( "June" ,(second (first android-quotes)))
        '( "July" ,(second (first android-quotes)))
        '( "Aug" ,(second (first android-quotes)))
        '( "Sep" ,(second (first android-quotes)))
        '( "Oct" ,(second (first android-quotes)))
        '( "Nov" ,(second (first android-quotes)))
        '("Dec" ,(second (second iphone-quotes)))))
(parameterize ([plot-height 200])
  (plot (discrete-histogram
	 (aggregate sum ($ jan-sentiment 'sentiment) ($ jan-sentiment 'freq))
	 #:y-min 0
	 #:y-max 8000
	 #:invert? #t
          #:label "Jan"
	 #:color "green"
	 #:line-color "MediumOrchid")
         
	#:x-label "Frequency"
	#:y-label "Sentiment Polarity"))

(parameterize ([plot-height 200])
  (plot (discrete-histogram
	 (aggregate sum ($ may-sentiment 'sentiment) ($ may-sentiment 'freq))
	 #:y-min 0
	 #:y-max 8000
	 #:invert? #t
	 #:color "green"
          #:label "May"
	 #:line-color "blue")
         
	#:x-label "Frequency"
	#:y-label "Sentiment Polarity"))

(parameterize ([plot-height 200])
  (plot (discrete-histogram
	 (aggregate sum ($ june-sentiment 'sentiment) ($ june-sentiment 'freq))
	 #:y-min 0
	 #:y-max 8000
	 #:invert? #t
          #:label "June"
	 #:color "green"
	 #:line-color "MediumOrchid")
         
	#:x-label "Frequency"
	#:y-label "Sentiment Polarity"))

(parameterize ([plot-height 200])
  (plot (discrete-histogram
	 (aggregate sum ($ july-sentiment 'sentiment) ($ july-sentiment 'freq))
	 #:y-min 0
	 #:y-max 8000
	 #:invert? #t
	 #:color "orange"
          #:label "July"
	 #:line-color "MediumOrchid")
         
	#:x-label "Frequency"
	#:y-label "Sentiment Polarity"))

(parameterize ([plot-height 200])
  (plot (discrete-histogram
	 (aggregate sum ($ august-sentiment 'sentiment) ($ august-sentiment 'freq))
	 #:y-min 0
	 #:y-max 8000
	 #:invert? #t
	 #:color "green"
         #:label "August"
	 #:line-color "green")
         
	#:x-label "Frequency"
	#:y-label "Sentiment Polarity"))
(parameterize ([plot-height 200])
  (plot (discrete-histogram
	 (aggregate sum ($ september-sentiment 'sentiment) ($ september-sentiment 'freq))
	 #:y-min 0
	 #:y-max 8000
	 #:invert? #t
         #:label "september"
	 #:color "green"
	 #:line-color "red")
         
	#:x-label "Frequency"
	#:y-label "Sentiment Polarity"))

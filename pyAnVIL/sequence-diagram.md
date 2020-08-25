
![image](https://user-images.githubusercontent.com/47808/71682339-36e19180-2d44-11ea-9add-cf71181cd07f.png)



# sequence

```
# https://sequencediagram.org/
title pyAnVIL single sign on

participantgroup #lightgreen **python client**
participant app
participant gen3_auth
participant gen3_submission
end

app->gen3_auth:init(terra_endpoint)

app->gen3_submission:init(gen3_auth)


app->gen3_submission:query(...)
activate gen3_submission

gen3_submission->gen3_auth:request.callback
activate gen3_auth
gen3_auth->gen3_submission:insert Authorization header
deactivate gen3_auth
gen3_submission->gen3_endpoint:query(...)
alt authorization error

gen3_endpoint-->gen3_submission:401
activate gen3_auth
gen3_submission->gen3_auth:request.callback


note right of gen3_auth: refresh token

gen3_auth->terra_endpoint:fence/accesstoken
terra_endpoint-->gen3_auth: access_token
gen3_auth->gen3_submission:insert Authorization header
note right of gen3_auth: retry
gen3_auth->gen3_endpoint:query(...)

end
deactivate gen3_auth

gen3_endpoint-->gen3_submission:data ...


```
